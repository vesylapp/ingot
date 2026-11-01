//! Tree-walking evaluator implementing Inngest's CEL semantics.
//!
//! Every function/operator call goes through [`Evaluator::call`], which mirrors
//! Inngest's `runtimeUnknownCall` decorator (unknown/error/null handling and
//! cross-type coercion) before dispatching to standard CEL behaviour in
//! [`Evaluator::native`].

use std::cmp::Ordering;
use std::rc::Rc;

use base64::Engine as _;
use chrono::{DateTime, Datelike, FixedOffset, TimeDelta, TimeZone, Timelike, Utc};

use crate::timefmt::Ts;
use serde_json::Value as Json;

use crate::compile::{
    Attr, Base, Comp, CompKind, Const, Func, Node, Program, Qual, Root, compile_regex,
};
use crate::timefmt;
use crate::value::{Bytes, Kind, List, MapV, Str, Val, cel_cmp, cel_eq, go_float};

pub(crate) struct Evaluator<'a> {
    prog: &'a Program,
    event: Option<&'a Json>,
    async_: Option<&'a Json>,
    /// Lazily computed: for each pattern, whether its path is absent from the input.
    missing: Option<Vec<bool>>,
    locals: Vec<Val<'a>>,
    /// While > 0 (inside `has()`), unknown patterns are not applied.
    no_unknowns: u32,
    now: Option<Ts>,
}

/// -2^63 as a double (exact); `[-2^63, 2^63)` is the range convertible to i64.
pub(crate) const I64_MIN_F: f64 = i64::MIN as f64;

fn const_val(c: &Const) -> Val<'_> {
    match c {
        Const::Null => Val::Null,
        Const::Bool(b) => Val::Bool(*b),
        Const::Int(i) => Val::Int(*i),
        Const::Uint(u) => Val::Uint(*u),
        Const::Double(d) => Val::Double(*d),
        Const::Str(s) => Val::Str(Str::B(s)),
        Const::Bytes(b) => Val::Bytes(Bytes::B(b)),
        Const::Type(k) => Val::Type(*k),
    }
}

/// Go type names as printed by cel-go's "expected iterable type" error.
fn go_type_name(v: &Val<'_>) -> &'static str {
    match v {
        Val::Null => "types.Null",
        Val::Bool(_) => "types.Bool",
        Val::Int(_) => "types.Int",
        Val::Uint(_) => "types.Uint",
        Val::Double(_) => "types.Double",
        Val::Str(_) => "types.String",
        Val::Bytes(_) => "types.Bytes",
        Val::Ts(_) => "types.Timestamp",
        Val::Dur(_) => "types.Duration",
        Val::Type(_) => "*types.Type",
        _ => "unknown",
    }
}

fn path_exists(root: Option<&Json>, path: &[std::sync::Arc<str>]) -> bool {
    let Some(mut cur) = root else {
        return false;
    };
    for (i, key) in path.iter().enumerate() {
        let Json::Object(m) = cur else {
            return false;
        };
        match m.get(&**key) {
            None => return false,
            Some(v) => {
                if i == path.len() - 1 {
                    return true;
                }
                cur = v;
            }
        }
    }
    false
}

fn b64_engine() -> base64::engine::GeneralPurpose {
    base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireCanonical),
    )
}

/// Go's `base64.StdEncoding.DecodeString`, keeping the bytes decoded before
/// any error (Inngest ignores the error).
fn go_b64decode(s: &str) -> String {
    let clean: Vec<u8> = s.bytes().filter(|b| *b != b'\r' && *b != b'\n').collect();
    let eng = b64_engine();
    if let Ok(v) = eng.decode(&clean) {
        return String::from_utf8_lossy(&v).into_owned();
    }
    let mut out = Vec::new();
    for chunk in clean.chunks(4) {
        if chunk.len() < 4 {
            break;
        }
        match eng.decode(chunk) {
            Ok(v) => {
                out.extend_from_slice(&v);
                if chunk.contains(&b'=') {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn ts_add<'a>(t: &Ts, nanos: i64) -> Val<'a> {
    match t.checked_add_signed(TimeDelta::nanoseconds(nanos)) {
        Some(r) if (1..=9999).contains(&r.year()) => Val::Ts(r),
        _ => Val::err("timestamp overflow"),
    }
}

fn index_of<'a>(key: &Val<'_>) -> Result<i64, Val<'a>> {
    match key {
        Val::Int(i) => Ok(*i),
        Val::Uint(u) => i64::try_from(*u).map_err(|_| Val::err(format!("unsupported index value {u} in list"))),
        Val::Double(d) => {
            if d.fract() == 0.0 && *d >= i64::MIN as f64 && *d < i64::MAX as f64 {
                Ok(*d as i64)
            } else {
                Err(Val::err(format!("unsupported index value {} in list", go_float(*d))))
            }
        }
        other => Err(Val::err(format!(
            "unsupported index type '{}' in list",
            other.kind().name()
        ))),
    }
}

/// cel-go `refQualify` (non-presence): select a key/index from a value.
fn qualify<'a>(obj: Val<'a>, key: &Val<'a>) -> Val<'a> {
    if key.is_unknown_or_err() {
        return key.clone();
    }
    match obj {
        Val::Unknown | Val::Err(_) => obj,
        Val::Map(m) => match m.get(key) {
            Ok(Some(v)) => v,
            Ok(None) => Val::err(format!("no such key: {}", key.go_string())),
            Err(e) => Val::Err(e),
        },
        Val::List(l) => match index_of(key) {
            Ok(i) if i >= 0 && (i as u64) < l.len() as u64 => l.get(i as usize).unwrap_or(Val::Null),
            Ok(i) => Val::err(format!("index out of bounds: {i}")),
            Err(e) => e,
        },
        _ => Val::err(format!("no such key: {}", key.go_string())),
    }
}

impl<'a> Evaluator<'a> {
    pub(crate) fn new(prog: &'a Program, event: Option<&'a Json>, async_: Option<&'a Json>) -> Self {
        Evaluator {
            prog,
            event,
            async_,
            missing: None,
            locals: Vec::new(),
            no_unknowns: 0,
            now: None,
        }
    }

    pub(crate) fn run(&mut self) -> Val<'a> {
        let root = &self.prog.root;
        self.eval(root)
    }

    fn eval(&mut self, n: &'a Node) -> Val<'a> {
        match n {
            Node::Const(c) => const_val(c),
            Node::Attr(a) => self.eval_attr(a),
            Node::Has(a, f) => self.eval_has(a, f),
            Node::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for i in items {
                    let v = self.eval(i);
                    if v.is_unknown_or_err() {
                        return v;
                    }
                    out.push(v);
                }
                Val::List(List::Owned(Rc::new(out)))
            }
            Node::Map(entries) => {
                let mut out: Vec<(Val<'a>, Val<'a>)> = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    let kv = self.eval(k);
                    if kv.is_unknown_or_err() {
                        return kv;
                    }
                    let vv = self.eval(v);
                    if vv.is_unknown_or_err() {
                        return vv;
                    }
                    if !matches!(kv, Val::Str(_) | Val::Int(_) | Val::Uint(_) | Val::Bool(_)) {
                        return Val::err(format!("unsupported key type: {}", kv.kind().name()));
                    }
                    // Duplicate keys: the last one wins (map literal keys are
                    // lifted into variables in Inngest, so cel-go never rejects them).
                    if let Some(slot) = out.iter_mut().find(|(e, _)| cel_eq(e, &kv) && e.kind() == kv.kind()) {
                        slot.1 = vv;
                    } else {
                        out.push((kv, vv));
                    }
                }
                Val::Map(MapV::Owned(Rc::new(out)))
            }
            Node::And(a, b) => {
                // JS-like: the first falsy operand, else the last (equivalent
                // to CEL's && when both operands are bools).
                let l = self.eval(a);
                if !l.truthy() {
                    return l;
                }
                self.eval(b)
            }
            Node::Or(a, b) => {
                let l = self.eval(a);
                if l.truthy() {
                    return l;
                }
                self.eval(b)
            }
            Node::Cond(c, t, f) => {
                if self.eval(c).truthy() {
                    self.eval(t)
                } else {
                    self.eval(f)
                }
            }
            Node::Call(f, args) => match args.as_slice() {
                [] => self.call(*f, &[]),
                [a] => {
                    let a = self.eval(a);
                    self.call(*f, &[a])
                }
                [a, b] => {
                    let a = self.eval(a);
                    let b = self.eval(b);
                    self.call(*f, &[a, b])
                }
                _ => {
                    let vals: Vec<Val<'a>> = args.iter().map(|a| self.eval(a)).collect();
                    self.call(*f, &vals)
                }
            },
            Node::Comp(c) => self.eval_comp(c),
        }
    }

    // -- attributes ---------------------------------------------------------

    fn root_val(&self, r: Root, quals: &[Qual]) -> Val<'a> {
        let v = match r {
            Root::Event => self.event,
            Root::Async => self.async_,
            Root::Vars => None,
        };
        match v {
            Some(j) => Val::from_json(j),
            None => {
                // cel-go names the longest dotted (namespaced) identifier.
                let mut name = r.name().to_string();
                for q in quals {
                    let Qual::Field(f) = q else { break };
                    name.push('.');
                    name.push_str(f);
                }
                Val::err(format!("no such attribute(s): {name}"))
            }
        }
    }

    fn missing(&mut self) -> &[bool] {
        if self.missing.is_none() {
            let m = self
                .prog
                .patterns
                .iter()
                .map(|p| {
                    let root = match p.root {
                        Root::Event => self.event,
                        Root::Async => self.async_,
                        Root::Vars => None,
                    };
                    !path_exists(root, &p.path)
                })
                .collect();
            self.missing = Some(m);
        }
        self.missing.as_deref().unwrap_or(&[])
    }

    fn has_missing_for(&mut self, r: Root) -> bool {
        if self.prog.patterns.is_empty() {
            return false;
        }
        let prog = self.prog;
        let missing = self.missing();
        prog.patterns
            .iter()
            .zip(missing)
            .any(|(p, m)| *m && p.root == r)
    }

    fn matches_missing(&mut self, r: Root, quals: &[Val<'a>]) -> bool {
        let prog = self.prog;
        let missing = self.missing();
        for (p, m) in prog.patterns.iter().zip(missing) {
            if !*m || p.root != r {
                continue;
            }
            if quals.is_empty() {
                return true;
            }
            let n = quals.len().min(p.path.len());
            if (0..n).all(|j| matches!(&quals[j], Val::Str(s) if s.as_str() == &*p.path[j])) {
                return true;
            }
        }
        false
    }

    fn qual_val(&mut self, q: &'a Qual) -> Val<'a> {
        match q {
            Qual::Field(f) => Val::Str(Str::B(f)),
            Qual::Const(c) => const_val(c),
            Qual::Dyn(n) => self.eval(n),
        }
    }

    fn apply_quals(&mut self, mut cur: Val<'a>, quals: &'a [Qual]) -> Val<'a> {
        for q in quals {
            if cur.is_unknown_or_err() {
                return cur;
            }
            let k = self.qual_val(q);
            cur = qualify(cur, &k);
        }
        cur
    }

    fn eval_attr(&mut self, a: &'a Attr) -> Val<'a> {
        match &a.base {
            Base::Root(r) => {
                if self.no_unknowns == 0 && self.has_missing_for(*r) {
                    let mut qvals = Vec::with_capacity(a.quals.len());
                    for q in &a.quals {
                        let v = self.qual_val(q);
                        if v.is_unknown_or_err() {
                            return v;
                        }
                        qvals.push(v);
                    }
                    if self.matches_missing(*r, &qvals) {
                        return Val::Unknown;
                    }
                    let mut cur = self.root_val(*r, &a.quals);
                    for k in &qvals {
                        if cur.is_unknown_or_err() {
                            return cur;
                        }
                        cur = qualify(cur, k);
                    }
                    return cur;
                }
                let base = self.root_val(*r, &a.quals);
                self.apply_quals(base, &a.quals)
            }
            Base::Local(slot) => {
                let base = self.locals.get(*slot).cloned().unwrap_or(Val::Unknown);
                self.apply_quals(base, &a.quals)
            }
            Base::Expr(e) => {
                let base = self.eval(e);
                self.apply_quals(base, &a.quals)
            }
        }
    }

    fn eval_has(&mut self, a: &'a Attr, field: &str) -> Val<'a> {
        // Presence tests are evaluated against the concrete data, without
        // unknown patterns (Inngest's evalPresenceTest).
        self.no_unknowns += 1;
        let base = self.eval_attr(a);
        self.no_unknowns -= 1;
        let r = match base {
            Val::Map(m) => Val::Bool(m.get_str(field).is_some()),
            Val::List(_) => Val::err("unsupported index type 'string' in list"),
            Val::Unknown | Val::Err(_) => base,
            _ => Val::Bool(false),
        };
        match &r {
            Val::Err(e) if e.starts_with("no such key:") || e.starts_with("no such attribute(s):") => {
                Val::Bool(false)
            }
            _ => r,
        }
    }

    // -- comprehensions -----------------------------------------------------

    fn eval_comp(&mut self, c: &'a Comp) -> Val<'a> {
        let range = self.eval(&c.range);
        let (list, keys) = match range {
            Val::Unknown | Val::Err(_) => return range,
            Val::List(l) => (Some(l), Vec::new()),
            Val::Map(m) => (None, m.keys()),
            other => {
                return Val::err(format!("got '{}', expected iterable type", go_type_name(&other)));
            }
        };
        let len = list.as_ref().map_or(keys.len(), List::len);
        let slot = c.slot;
        // The macros are evaluated exactly like cel-go's expansions, with every
        // operator going through Inngest's call semantics.
        let mut accu = match c.kind {
            CompKind::Exists => Val::Bool(false),
            CompKind::All => Val::Bool(true),
            CompKind::ExistsOne => Val::Int(0),
            CompKind::Map | CompKind::Filter => Val::List(List::Owned(Rc::new(Vec::new()))),
        };
        for i in 0..len {
            let elem = match &list {
                Some(l) => l.get(i),
                None => keys.get(i).cloned(),
            }
            .unwrap_or(Val::Null);
            let cond = match c.kind {
                CompKind::Exists => {
                    let n = self.call(Func::Not, &[accu.clone()]);
                    self.call(Func::NotStrictlyFalse, &[n])
                }
                CompKind::All => self.call(Func::NotStrictlyFalse, &[accu.clone()]),
                _ => Val::Bool(true),
            };
            if let Val::Bool(false) = cond {
                break;
            }
            self.locals.truncate(slot);
            self.locals.push(elem);
            accu = match c.kind {
                CompKind::Exists => {
                    if accu.truthy() {
                        accu
                    } else {
                        self.eval(&c.body)
                    }
                }
                CompKind::All => {
                    if !accu.truthy() {
                        accu
                    } else {
                        self.eval(&c.body)
                    }
                }
                CompKind::ExistsOne => {
                    if self.eval(&c.body).truthy() {
                        self.call(Func::Add, &[accu, Val::Int(1)])
                    } else {
                        accu
                    }
                }
                CompKind::Map => {
                    let keep = match &c.filter {
                        Some(f) => self.eval(f).truthy(),
                        None => true,
                    };
                    if keep {
                        let v = self.eval(&c.body);
                        self.append(accu, v)
                    } else {
                        accu
                    }
                }
                CompKind::Filter => {
                    if self.eval(&c.body).truthy() {
                        let x = self.locals.get(slot).cloned().unwrap_or(Val::Null);
                        self.append(accu, x)
                    } else {
                        accu
                    }
                }
            };
        }
        self.locals.truncate(slot);
        match c.kind {
            CompKind::ExistsOne => self.call(Func::Eq, &[accu, Val::Int(1)]),
            _ => accu,
        }
    }

    /// `accu + [v]`, with an in-place fast path for the common case.
    fn append(&mut self, accu: Val<'a>, v: Val<'a>) -> Val<'a> {
        if v.is_unknown_or_err() {
            // `[v]` evaluates to `v` itself.
            return self.call(Func::Add, &[accu, v]);
        }
        match accu {
            Val::List(List::Owned(mut rc)) => {
                Rc::make_mut(&mut rc).push(v);
                Val::List(List::Owned(rc))
            }
            other => {
                let item = Val::List(List::Owned(Rc::new(vec![v])));
                self.call(Func::Add, &[other, item])
            }
        }
    }

    // -- calls --------------------------------------------------------------

    /// Inngest's `runtimeUnknownCall` wrapper around every call.
    fn call(&mut self, f: Func, args: &[Val<'a>]) -> Val<'a> {
        if args.iter().any(Val::is_go_nil) {
            return call_with_go_nil(f, args);
        }
        // Track up to two distinct argument types, like Inngest's argColl.
        let mut t0 = Kind::Null;
        let mut t1 = Kind::Null;
        let mut nt = 0;
        for a in args {
            let k = a.kind();
            match nt {
                0 => {
                    t0 = k;
                    nt = 1;
                }
                1 if k != t0 => {
                    t1 = k;
                    nt = 2;
                }
                _ => {}
            }
        }
        let exists = |k: Kind| (nt >= 1 && t0 == k) || (nt >= 2 && t1 == k);
        let has_unknown = exists(Kind::Unknown);
        let has_err = exists(Kind::Err);

        if nt == 1 && !has_err && !has_unknown {
            return self.native(f, args);
        }
        if nt == 2 && !has_unknown {
            let v = self.native(f, args);
            if !v.is_unknown_or_err() {
                return v;
            }
            if f == Func::Add {
                // Concatenate distinct types as strings (Go `%v%v`).
                let mut s = args[0].go_string();
                s.push_str(&args[1].go_string());
                return Val::string(s);
            }
        }
        if has_err || has_unknown {
            for a in args.iter().take(2) {
                if let Val::Err(e) = a
                    && e.starts_with("no such attribute")
                {
                    return self.native(f, args);
                }
            }
            return handle_unknown(f, args, exists(Kind::Null));
        }
        if exists(Kind::Null) && args.len() == 2 {
            match f {
                Func::Eq => return Val::Bool(false),
                Func::Ne => return Val::Bool(true),
                Func::Lt | Func::Le | Func::Gt | Func::Ge | Func::Add => {
                    // Replace the null with the zero value of the other type.
                    let other = if t0 == Kind::Null { t1 } else { t0 };
                    let zero = match other {
                        Kind::Int => Val::Int(0),
                        Kind::Uint => Val::Uint(0),
                        Kind::Double => Val::Double(0.0),
                        Kind::String => Val::Str(Str::B("")),
                        _ => Val::Null,
                    };
                    let zeroed: Vec<Val<'a>> = args
                        .iter()
                        .map(|a| if matches!(a, Val::Null) { zero.clone() } else { a.clone() })
                        .collect();
                    let comparable = |v: &Val<'_>| {
                        matches!(
                            v,
                            Val::Int(_) | Val::Uint(_) | Val::Double(_) | Val::Str(_) | Val::Bytes(_)
                                | Val::Bool(_) | Val::Ts(_) | Val::Dur(_)
                        )
                    };
                    if f != Func::Add && !comparable(&zeroed[0]) {
                        // cel-go asserts traits.Comparer here and panics.
                        return Val::go_nil("comparison with null");
                    }
                    return self.native(f, &zeroed);
                }
                _ => {}
            }
        }
        self.native(f, args)
    }

    fn now(&mut self) -> Ts {
        *self.now.get_or_insert_with(|| Utc::now().fixed_offset())
    }

    /// Standard CEL function semantics (strict in their arguments).
    fn native(&mut self, f: Func, args: &[Val<'a>]) -> Val<'a> {
        if f == Func::NotStrictlyFalse {
            return match args.first() {
                Some(Val::Bool(b)) => Val::Bool(*b),
                _ => Val::Bool(true),
            };
        }
        if let Some(bad) = args.iter().find(|a| a.is_unknown_or_err()) {
            return bad.clone();
        }
        // cel-go reports "no such overload" when the left operand supports the
        // operation but not with this right operand, and names the function
        // otherwise.
        let lhs_supports = match (f, args.first()) {
            (Func::Add, Some(v)) => matches!(
                v,
                Val::Int(_) | Val::Uint(_) | Val::Double(_) | Val::Str(_) | Val::Bytes(_) | Val::List(_)
                    | Val::Ts(_) | Val::Dur(_)
            ),
            (Func::Sub, Some(v)) => matches!(v, Val::Int(_) | Val::Uint(_) | Val::Double(_) | Val::Ts(_) | Val::Dur(_)),
            (Func::Mul | Func::Div, Some(v)) => matches!(v, Val::Int(_) | Val::Uint(_) | Val::Double(_)),
            (Func::Mod, Some(v)) => matches!(v, Val::Int(_) | Val::Uint(_)),
            (Func::Lt | Func::Le | Func::Gt | Func::Ge, Some(v)) => matches!(
                v,
                Val::Int(_) | Val::Uint(_) | Val::Double(_) | Val::Str(_) | Val::Bytes(_) | Val::Bool(_)
                    | Val::Ts(_) | Val::Dur(_)
            ),
            (Func::Size, _) => false,
            _ => true,
        };
        let no = || {
            if lhs_supports {
                Val::err("no such overload")
            } else {
                Val::no_overload(f.name())
            }
        };
        match f {
            Func::Add => match (&args[0], &args[1]) {
                (Val::Int(a), Val::Int(b)) => a.checked_add(*b).map_or_else(|| Val::err("integer overflow"), Val::Int),
                (Val::Uint(a), Val::Uint(b)) => {
                    a.checked_add(*b).map_or_else(|| Val::err("unsigned integer overflow"), Val::Uint)
                }
                (Val::Double(a), Val::Double(b)) => Val::Double(a + b),
                (Val::Str(a), Val::Str(b)) => {
                    let mut s = String::with_capacity(a.as_str().len() + b.as_str().len());
                    s.push_str(a.as_str());
                    s.push_str(b.as_str());
                    Val::string(s)
                }
                (Val::Bytes(a), Val::Bytes(b)) => {
                    let mut v = a.as_slice().to_vec();
                    v.extend_from_slice(b.as_slice());
                    Val::Bytes(Bytes::O(Rc::from(v)))
                }
                (Val::List(a), Val::List(b)) => {
                    let mut v: Vec<Val<'a>> = Vec::with_capacity(a.len() + b.len());
                    v.extend(a.iter());
                    v.extend(b.iter());
                    Val::List(List::Owned(Rc::new(v)))
                }
                (Val::Ts(t), Val::Dur(d)) | (Val::Dur(d), Val::Ts(t)) => ts_add(t, *d),
                (Val::Dur(a), Val::Dur(b)) => a.checked_add(*b).map_or_else(|| Val::err("integer overflow"), Val::Dur),
                _ => no(),
            },
            Func::Sub => match (&args[0], &args[1]) {
                (Val::Int(a), Val::Int(b)) => a.checked_sub(*b).map_or_else(|| Val::err("integer overflow"), Val::Int),
                (Val::Uint(a), Val::Uint(b)) => {
                    a.checked_sub(*b).map_or_else(|| Val::err("unsigned integer overflow"), Val::Uint)
                }
                (Val::Double(a), Val::Double(b)) => Val::Double(a - b),
                (Val::Ts(a), Val::Ts(b)) => (*a - *b)
                    .num_nanoseconds()
                    .map_or_else(|| Val::err("integer overflow"), Val::Dur),
                (Val::Ts(t), Val::Dur(d)) => match d.checked_neg() {
                    Some(n) => ts_add(t, n),
                    None => Val::err("integer overflow"),
                },
                (Val::Dur(a), Val::Dur(b)) => a.checked_sub(*b).map_or_else(|| Val::err("integer overflow"), Val::Dur),
                _ => no(),
            },
            Func::Mul => match (&args[0], &args[1]) {
                (Val::Int(a), Val::Int(b)) => a.checked_mul(*b).map_or_else(|| Val::err("integer overflow"), Val::Int),
                (Val::Uint(a), Val::Uint(b)) => {
                    a.checked_mul(*b).map_or_else(|| Val::err("unsigned integer overflow"), Val::Uint)
                }
                (Val::Double(a), Val::Double(b)) => Val::Double(a * b),
                _ => no(),
            },
            Func::Div => match (&args[0], &args[1]) {
                (Val::Int(_), Val::Int(0)) | (Val::Uint(_), Val::Uint(0)) => Val::err("division by zero"),
                (Val::Int(a), Val::Int(b)) => a.checked_div(*b).map_or_else(|| Val::err("integer overflow"), Val::Int),
                (Val::Uint(a), Val::Uint(b)) => Val::Uint(a / b),
                (Val::Double(a), Val::Double(b)) => Val::Double(a / b),
                _ => no(),
            },
            Func::Mod => match (&args[0], &args[1]) {
                (Val::Int(_), Val::Int(0)) | (Val::Uint(_), Val::Uint(0)) => Val::err("modulus by zero"),
                (Val::Int(a), Val::Int(b)) => a.checked_rem(*b).map_or_else(|| Val::err("integer overflow"), Val::Int),
                (Val::Uint(a), Val::Uint(b)) => Val::Uint(a % b),
                _ => no(),
            },
            Func::Neg => match &args[0] {
                Val::Int(i) => i.checked_neg().map_or_else(|| Val::err("integer overflow"), Val::Int),
                Val::Double(d) => Val::Double(-d),
                _ => no(),
            },
            Func::Not => match &args[0] {
                Val::Bool(b) => Val::Bool(!b),
                _ => no(),
            },
            Func::Eq => Val::Bool(cel_eq(&args[0], &args[1])),
            Func::Ne => Val::Bool(!cel_eq(&args[0], &args[1])),
            Func::Lt | Func::Le | Func::Gt | Func::Ge => match cel_cmp(&args[0], &args[1]) {
                Ok(o) => Val::Bool(match f {
                    Func::Lt => o == Ordering::Less,
                    Func::Le => o != Ordering::Greater,
                    Func::Gt => o == Ordering::Greater,
                    _ => o != Ordering::Less,
                }),
                Err(Val::Err(m)) if m.starts_with("NaN") => Val::Err(m),
                Err(_) => no(),
            },
            Func::In => match &args[1] {
                Val::List(l) => Val::Bool(l.iter().any(|v| cel_eq(&args[0], &v))),
                Val::Map(m) => match m.get(&args[0]) {
                    Ok(found) => Val::Bool(found.is_some()),
                    Err(e) => Val::Err(e),
                },
                _ => no(),
            },
            Func::NotStrictlyFalse => Val::Bool(true),
            Func::Size => match &args[0] {
                Val::Str(s) => Val::Int(s.as_str().chars().count() as i64),
                Val::Bytes(b) => Val::Int(b.as_slice().len() as i64),
                Val::List(l) => Val::Int(l.len() as i64),
                Val::Map(m) => Val::Int(m.len() as i64),
                _ => no(),
            },
            Func::Contains | Func::StartsWith | Func::EndsWith => match (&args[0], &args[1]) {
                (Val::Str(a), Val::Str(b)) => {
                    let (a, b) = (a.as_str(), b.as_str());
                    Val::Bool(match f {
                        Func::Contains => a.contains(b),
                        Func::StartsWith => a.starts_with(b),
                        _ => a.ends_with(b),
                    })
                }
                _ => no(),
            },
            Func::Matches | Func::MatchesLit(_) => match (&args[0], &args[1]) {
                (Val::Str(s), Val::Str(p)) => {
                    if let Func::MatchesLit(i) = f
                        && let Some(re) = self.prog.regexes.get(i)
                    {
                        return match re {
                            Ok(re) => Val::Bool(re.is_match(s.as_str())),
                            Err(e) => Val::err(e.clone()),
                        };
                    }
                    match compile_regex(p.as_str()) {
                        Ok(re) => Val::Bool(re.is_match(s.as_str())),
                        Err(e) => Val::err(e),
                    }
                }
                _ => no(),
            },
            Func::ToInt => match &args[0] {
                Val::Int(i) => Val::Int(*i),
                Val::Uint(u) => i64::try_from(*u).map_or_else(|_| Val::err("integer overflow"), Val::Int),
                Val::Double(d) => {
                    if (I64_MIN_F..-I64_MIN_F).contains(d) {
                        Val::Int(d.trunc() as i64)
                    } else {
                        Val::err("integer overflow")
                    }
                }
                Val::Str(s) => s
                    .as_str()
                    .parse::<i64>()
                    .map_or_else(|_| Val::err("type conversion error from 'string' to 'int'"), Val::Int),
                Val::Ts(t) => Val::Int(t.timestamp()),
                Val::Dur(d) => Val::Int(*d),
                other => Val::err(format!("type conversion error from '{}' to 'int'", other.kind().name())),
            },
            Func::ToUint => match &args[0] {
                Val::Int(i) => u64::try_from(*i).map_or_else(|_| Val::err("unsigned integer overflow"), Val::Uint),
                Val::Uint(u) => Val::Uint(*u),
                Val::Double(d) => {
                    if d.is_finite() && *d >= 0.0 && *d < 1.844_674_407_370_955_2e19 {
                        Val::Uint(d.trunc() as u64)
                    } else {
                        Val::err("unsigned integer overflow")
                    }
                }
                Val::Str(s) => s
                    .as_str()
                    .parse::<u64>()
                    .map_or_else(|_| Val::err("type conversion error from 'string' to 'uint'"), Val::Uint),
                other => Val::err(format!("type conversion error from '{}' to 'uint'", other.kind().name())),
            },
            Func::ToDouble => match &args[0] {
                Val::Int(i) => Val::Double(*i as f64),
                Val::Uint(u) => Val::Double(*u as f64),
                Val::Double(d) => Val::Double(*d),
                Val::Str(s) => s
                    .as_str()
                    .parse::<f64>()
                    .map_or_else(|_| Val::err("type conversion error from 'string' to 'double'"), Val::Double),
                other => Val::err(format!("type conversion error from '{}' to 'double'", other.kind().name())),
            },
            Func::ToString => match &args[0] {
                Val::Str(s) => Val::Str(s.clone()),
                Val::Bool(b) => Val::Str(Str::B(if *b { "true" } else { "false" })),
                Val::Int(i) => Val::string(i.to_string()),
                Val::Uint(u) => Val::string(u.to_string()),
                Val::Double(d) => Val::string(go_float(*d)),
                Val::Bytes(b) => match std::str::from_utf8(b.as_slice()) {
                    Ok(s) => Val::string(s.to_string()),
                    Err(_) => Val::err("invalid UTF-8 in bytes, cannot convert to string"),
                },
                Val::Ts(t) => Val::string(timefmt::rfc3339_nano(t)),
                Val::Dur(d) => Val::string(format!("{}s", *d as f64 / 1e9)),
                other => Val::err(format!("type conversion error from '{}' to 'string'", other.kind().name())),
            },
            Func::ToBool => match &args[0] {
                Val::Bool(b) => Val::Bool(*b),
                Val::Str(s) => match s.as_str() {
                    "1" | "t" | "T" | "TRUE" | "true" | "True" => Val::Bool(true),
                    "0" | "f" | "F" | "FALSE" | "false" | "False" => Val::Bool(false),
                    _ => Val::err("type conversion error from 'string' to 'bool'"),
                },
                other => Val::err(format!("type conversion error from '{}' to 'bool'", other.kind().name())),
            },
            Func::ToBytes => match &args[0] {
                Val::Str(s) => Val::Bytes(Bytes::O(Rc::from(s.as_str().as_bytes()))),
                Val::Bytes(b) => Val::Bytes(b.clone()),
                other => Val::err(format!("type conversion error from '{}' to 'bytes'", other.kind().name())),
            },
            Func::Dyn => args[0].clone(),
            Func::TypeOf => Val::Type(args[0].kind()),
            Func::Timestamp => match &args[0] {
                Val::Str(s) => match timefmt::parse_rfc3339(s.as_str()) {
                    Ok(t) if (1..=9999).contains(&t.year()) => Val::Ts(t),
                    Ok(_) => Val::err("timestamp out of range"),
                    Err(e) => Val::err(e),
                },
                Val::Int(i) => match Utc.timestamp_opt(*i, 0).single().map(|t| t.fixed_offset()) {
                    Some(t) if (1..=9999).contains(&t.year()) => Val::Ts(t),
                    _ => Val::err("timestamp out of range"),
                },
                Val::Ts(t) => Val::Ts(*t),
                other => Val::err(format!("type conversion error from '{}' to 'google.protobuf.Timestamp'", other.kind().name())),
            },
            Func::Duration => match &args[0] {
                Val::Str(s) => match timefmt::parse_duration(s.as_str(), false) {
                    Ok(d) => Val::Dur(d),
                    Err(e) => Val::err(e),
                },
                Val::Dur(d) => Val::Dur(*d),
                other => Val::err(format!("type conversion error from '{}' to 'google.protobuf.Duration'", other.kind().name())),
            },
            Func::GetFullYear
            | Func::GetMonth
            | Func::GetDayOfYear
            | Func::GetDayOfMonth
            | Func::GetDate
            | Func::GetDayOfWeek
            | Func::GetHours
            | Func::GetMinutes
            | Func::GetSeconds
            | Func::GetMilliseconds => getter(f, args),
            Func::Lowercase | Func::Uppercase => {
                let s = match &args[0] {
                    Val::Str(s) => s.as_str(),
                    _ => "",
                };
                Val::string(if f == Func::Lowercase {
                    s.to_lowercase()
                } else {
                    s.to_uppercase()
                })
            }
            Func::B64Decode => match &args[0] {
                Val::Str(s) => Val::string(go_b64decode(s.as_str())),
                _ => Val::Str(Str::B("")),
            },
            Func::JsonParse => {
                let parsed = match &args[0] {
                    Val::Str(s) => serde_json::from_str::<serde_json::Map<String, Json>>(s.as_str()).ok(),
                    _ => None,
                };
                Val::from_json_owned(&Json::Object(parsed.unwrap_or_default()))
            }
            Func::Date => {
                let t = match &args[0] {
                    Val::Str(s) => timefmt::date_from_str(s.as_str()),
                    Val::Int(i) => timefmt::date_from_int(*i),
                    // Lenient: Go only accepts strings and ints here, but JSON
                    // numbers (e.g. `event.ts`) are doubles.
                    Val::Double(d) if d.is_finite() => timefmt::date_from_int(*d as i64),
                    _ => None,
                };
                t.map_or_else(|| Val::go_nil("date: unknown date format"), Val::Ts)
            }
            Func::Now => Val::Ts(self.now()),
            Func::NowPlus | Func::NowMinus => match &args[0] {
                Val::Str(s) => match timefmt::parse_duration(s.as_str(), true) {
                    Ok(d) => {
                        let now = self.now();
                        let d = if f == Func::NowMinus { d.checked_neg() } else { Some(d) };
                        match d {
                            Some(d) => ts_add(&now, d),
                            None => Val::err("integer overflow"),
                        }
                    }
                    Err(e) => Val::go_nil(&e),
                },
                _ => Val::go_nil("now_plus/now_minus require a string duration"),
            },
        }
    }
}

/// A call with a Go `nil` argument: nil contributes no type, so the call is
/// only survivable when an unknown/error argument routes it to
/// `handleUnknownCall`; otherwise cel-go panics and evaluation aborts.
fn call_with_go_nil<'a>(f: Func, args: &[Val<'a>]) -> Val<'a> {
    let nil = args.iter().find(|a| a.is_go_nil()).cloned().unwrap_or(Val::Unknown);
    let other_unknownish = args.iter().any(|a| !a.is_go_nil() && a.is_unknown_or_err());
    if !other_unknownish {
        return nil;
    }
    if matches!(f, Func::Lt | Func::Le | Func::Gt | Func::Ge) && args.first().is_some_and(Val::is_go_nil) {
        return nil;
    }
    handle_unknown(f, args, args.iter().any(|a| matches!(a, Val::Null)))
}

/// Inngest's `handleUnknownCall`: concrete results for calls with unknown or
/// error arguments.
fn handle_unknown<'a>(f: Func, args: &[Val<'a>], has_null: bool) -> Val<'a> {
    let first_unknownish = matches!(args.first(), Some(Val::Unknown | Val::Err(_)));
    match f {
        Func::Add => args
            .iter()
            .take(2)
            .find(|a| !matches!(a, Val::Unknown))
            .cloned()
            .unwrap_or(Val::Bool(false)),
        Func::Eq => Val::Bool(has_null),
        Func::Ne => Val::Bool(!has_null),
        Func::Lt | Func::Le => Val::Bool(first_unknownish),
        Func::Gt | Func::Ge => Val::Bool(!first_unknownish),
        Func::Size => Val::Int(0),
        _ => Val::Bool(false),
    }
}

fn tz_getter<T: TimeZone>(f: Func, t: DateTime<T>) -> i64 {
    match f {
        Func::GetFullYear => i64::from(t.year()),
        Func::GetMonth => i64::from(t.month0()),
        Func::GetDayOfYear => i64::from(t.ordinal0()),
        Func::GetDayOfMonth => i64::from(t.day0()),
        Func::GetDate => i64::from(t.day()),
        Func::GetDayOfWeek => i64::from(t.weekday().num_days_from_sunday()),
        Func::GetHours => i64::from(t.hour()),
        Func::GetMinutes => i64::from(t.minute()),
        Func::GetSeconds => i64::from(t.second()),
        _ => i64::from(t.nanosecond() % 1_000_000_000 / 1_000_000),
    }
}

fn getter<'a>(f: Func, args: &[Val<'a>]) -> Val<'a> {
    match (&args[0], args.get(1)) {
        (Val::Ts(t), None) => Val::Int(tz_getter(f, t.with_timezone(&Utc))),
        (Val::Ts(t), Some(Val::Str(tz))) => {
            let tz = tz.as_str();
            if let Some(idx) = tz.find(':') {
                // Numeric offset like "+05:30" / "-08:00".
                let (h, m) = (&tz[..idx], &tz[idx + 1..]);
                let (Ok(hr), Ok(min)) = (h.parse::<i32>(), m.parse::<i32>()) else {
                    return Val::err(format!("unknown time zone {tz}"));
                };
                let offset = if tz.starts_with('-') { hr * 60 - min } else { hr * 60 + min };
                match FixedOffset::east_opt(offset * 60) {
                    Some(o) => Val::Int(tz_getter(f, t.with_timezone(&o))),
                    None => Val::err(format!("unknown time zone {tz}")),
                }
            } else if tz.is_empty() || tz == "UTC" {
                Val::Int(tz_getter(f, t.with_timezone(&Utc)))
            } else {
                match tz.parse::<chrono_tz::Tz>() {
                    Ok(z) => Val::Int(tz_getter(f, t.with_timezone(&z))),
                    Err(_) => Val::err(format!("unknown time zone {tz}")),
                }
            }
        }
        (Val::Dur(d), None) => match f {
            Func::GetHours => Val::Int(d / 3_600_000_000_000),
            Func::GetMinutes => Val::Int(d / 60_000_000_000),
            Func::GetSeconds => Val::Int(d / 1_000_000_000),
            Func::GetMilliseconds => Val::Int(d / 1_000_000),
            _ => Val::no_overload(f.name()),
        },
        _ => Val::no_overload(f.name()),
    }
}
