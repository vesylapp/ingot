//! Lowers the parse tree into an evaluation IR, resolving identifiers,
//! expanding macros, and computing Inngest's "unknown attribute" patterns.

use std::collections::HashSet;
use std::sync::Arc;

use regex::Regex;

use crate::parser::{Lit, PExpr, PKind};
use crate::value::Kind;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Root {
    Event,
    Async,
    Vars,
    /// A finished run's output (run search only).
    Output,
    /// A failed run's error (run search only).
    Error,
}

impl Root {
    /// Resolves a root variable. `output` and `error` are only declared when `run_roots` is set.
    fn from_name(s: &str, run_roots: bool) -> Option<Root> {
        match s {
            "event" => Some(Root::Event),
            "async" => Some(Root::Async),
            "vars" => Some(Root::Vars),
            "output" if run_roots => Some(Root::Output),
            "error" if run_roots => Some(Root::Error),
            _ => None,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Root::Event => "event",
            Root::Async => "async",
            Root::Vars => "vars",
            Root::Output => "output",
            Root::Error => "error",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Func {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Neg,
    Not,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotStrictlyFalse,
    Size,
    Contains,
    StartsWith,
    EndsWith,
    /// `matches` with a dynamic pattern.
    Matches,
    /// `matches` whose pattern is a string literal; index into the regex table.
    MatchesLit(usize),
    ToInt,
    ToUint,
    ToDouble,
    ToString,
    ToBool,
    ToBytes,
    Dyn,
    TypeOf,
    Timestamp,
    Duration,
    GetFullYear,
    GetMonth,
    GetDayOfYear,
    GetDayOfMonth,
    GetDate,
    GetDayOfWeek,
    GetHours,
    GetMinutes,
    GetSeconds,
    GetMilliseconds,
    Lowercase,
    Uppercase,
    B64Decode,
    JsonParse,
    Date,
    Now,
    NowPlus,
    NowMinus,
}

impl Func {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Func::Add => "_+_",
            Func::Sub => "_-_",
            Func::Mul => "_*_",
            Func::Div => "_/_",
            Func::Mod => "_%_",
            Func::Neg => "-_",
            Func::Not => "!_",
            Func::Eq => "_==_",
            Func::Ne => "_!=_",
            Func::Lt => "_<_",
            Func::Le => "_<=_",
            Func::Gt => "_>_",
            Func::Ge => "_>=_",
            Func::In => "@in",
            Func::NotStrictlyFalse => "@not_strictly_false",
            Func::Size => "size",
            Func::Contains => "contains",
            Func::StartsWith => "startsWith",
            Func::EndsWith => "endsWith",
            Func::Matches | Func::MatchesLit(_) => "matches",
            Func::ToInt => "int",
            Func::ToUint => "uint",
            Func::ToDouble => "double",
            Func::ToString => "string",
            Func::ToBool => "bool",
            Func::ToBytes => "bytes",
            Func::Dyn => "dyn",
            Func::TypeOf => "type",
            Func::Timestamp => "timestamp",
            Func::Duration => "duration",
            Func::GetFullYear => "getFullYear",
            Func::GetMonth => "getMonth",
            Func::GetDayOfYear => "getDayOfYear",
            Func::GetDayOfMonth => "getDayOfMonth",
            Func::GetDate => "getDate",
            Func::GetDayOfWeek => "getDayOfWeek",
            Func::GetHours => "getHours",
            Func::GetMinutes => "getMinutes",
            Func::GetSeconds => "getSeconds",
            Func::GetMilliseconds => "getMilliseconds",
            Func::Lowercase => "lowercase",
            Func::Uppercase => "uppercase",
            Func::B64Decode => "b64decode",
            Func::JsonParse => "json_parse",
            Func::Date => "date",
            Func::Now => "now",
            Func::NowPlus => "now_plus",
            Func::NowMinus => "now_minus",
        }
    }

    /// Functions permitted by the restrictive (cancelOn) policy: logic
    /// operators, comparisons, `in`, and int/uint/double conversions.
    fn restrictive_allowed(self) -> bool {
        matches!(
            self,
            Func::Not
                | Func::Eq
                | Func::Ne
                | Func::Lt
                | Func::Le
                | Func::Gt
                | Func::Ge
                | Func::In
                | Func::ToInt
                | Func::ToUint
                | Func::ToDouble
        )
    }
}

#[derive(Debug)]
pub(crate) enum Const {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    Type(Kind),
}

#[derive(Debug)]
pub(crate) enum Qual {
    Field(Arc<str>),
    Const(Const),
    Dyn(Node),
}

#[derive(Debug)]
pub(crate) enum Base {
    Root(Root),
    Local(usize),
    Expr(Node),
}

/// An attribute: a base followed by field selections / index qualifiers,
/// like cel-go's attributes.
#[derive(Debug)]
pub(crate) struct Attr {
    pub base: Base,
    pub quals: Vec<Qual>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompKind {
    All,
    Exists,
    ExistsOne,
    Map,
    Filter,
}

#[derive(Debug)]
pub(crate) struct Comp {
    pub kind: CompKind,
    pub range: Node,
    pub slot: usize,
    /// Map's optional filter (3-arg form).
    pub filter: Option<Node>,
    pub body: Node,
}

#[derive(Debug)]
pub(crate) enum Node {
    Const(Const),
    Attr(Box<Attr>),
    /// `has(attr.field)`.
    Has(Box<Attr>, Arc<str>),
    List(Vec<Node>),
    Map(Vec<(Node, Node)>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Cond(Box<Node>, Box<Node>, Box<Node>),
    Call(Func, Vec<Node>),
    Comp(Box<Comp>),
}

/// A select path (rooted at a variable) that Inngest marks as "unknown" when
/// it is absent from the input data.
#[derive(Debug)]
pub(crate) struct Pattern {
    pub root: Root,
    pub path: Vec<Arc<str>>,
}

#[derive(Debug)]
pub(crate) struct Program {
    pub root: Node,
    pub patterns: Vec<Pattern>,
    pub regexes: Vec<Result<Regex, String>>,
}

pub(crate) fn compile(p: &PExpr, run_roots: bool) -> Result<Program, String> {
    let mut l = Lowerer {
        scope: Vec::new(),
        regexes: Vec::new(),
        run_roots,
    };
    let root = l.lower(p)?;
    let patterns = collect_patterns(p, run_roots);
    Ok(Program {
        root,
        patterns,
        regexes: l.regexes,
    })
}

struct Lowerer {
    scope: Vec<String>,
    regexes: Vec<Result<Regex, String>>,
    run_roots: bool,
}

fn lit_const(l: &Lit) -> Const {
    match l {
        Lit::Null => Const::Null,
        Lit::Bool(b) => Const::Bool(*b),
        Lit::Int(i) => Const::Int(*i),
        Lit::Uint(u) => Const::Uint(*u),
        Lit::Double(d) => Const::Double(*d),
        Lit::Str(s) => Const::Str(s.clone()),
        Lit::Bytes(b) => Const::Bytes(b.clone()),
    }
}

fn type_ident(name: &str) -> Option<Kind> {
    Some(match name {
        "int" => Kind::Int,
        "uint" => Kind::Uint,
        "double" => Kind::Double,
        "bool" => Kind::Bool,
        "string" => Kind::String,
        "bytes" => Kind::Bytes,
        "list" => Kind::List,
        "map" => Kind::Map,
        "null_type" => Kind::Null,
        "type" => Kind::Type,
        _ => return None,
    })
}

pub(crate) fn compile_regex(pattern: &str) -> Result<Regex, String> {
    regex::RegexBuilder::new(pattern)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|e| format!("error parsing regexp: {e}"))
}

fn is_macro(name: &str, has_target: bool, args: &[PExpr]) -> bool {
    if !has_target {
        return false;
    }
    match name {
        "all" | "exists" | "exists_one" | "existsOne" | "filter" => args.len() == 2,
        "map" => args.len() == 2 || args.len() == 3,
        _ => false,
    }
}

impl Lowerer {
    fn lower(&mut self, p: &PExpr) -> Result<Node, String> {
        match &p.kind {
            PKind::Lit(l) => Ok(Node::Const(lit_const(l))),
            PKind::Ident(name) => self.ident(name),
            PKind::Select(operand, field) => {
                let base = self.lower(operand)?;
                Ok(push_qual(base, Qual::Field(Arc::from(field.as_str()))))
            }
            PKind::Index(operand, index) => {
                let base = self.lower(operand)?;
                let q = match &index.kind {
                    PKind::Lit(l) => Qual::Const(lit_const(l)),
                    _ => Qual::Dyn(self.lower(index)?),
                };
                Ok(push_qual(base, q))
            }
            PKind::List(items) => Ok(Node::List(
                items.iter().map(|i| self.lower(i)).collect::<Result<_, _>>()?,
            )),
            PKind::Map(entries) => Ok(Node::Map(
                entries
                    .iter()
                    .map(|(k, v)| Ok((self.lower(k)?, self.lower(v)?)))
                    .collect::<Result<_, String>>()?,
            )),
            PKind::Call { name, target, args } => self.call(name, target.as_deref(), args),
        }
    }

    fn ident(&mut self, name: &str) -> Result<Node, String> {
        if let Some(slot) = self.scope.iter().rposition(|s| s == name) {
            return Ok(Node::Attr(Box::new(Attr {
                base: Base::Local(slot),
                quals: Vec::new(),
            })));
        }
        if let Some(root) = Root::from_name(name, self.run_roots) {
            return Ok(Node::Attr(Box::new(Attr {
                base: Base::Root(root),
                quals: Vec::new(),
            })));
        }
        if let Some(k) = type_ident(name) {
            return Ok(Node::Const(Const::Type(k)));
        }
        Err(format!("undeclared reference to '{name}' (in container '')"))
    }

    fn call(&mut self, name: &str, target: Option<&PExpr>, args: &[PExpr]) -> Result<Node, String> {
        // Operators and special forms.
        match (name, target.is_some(), args.len()) {
            ("_&&_", false, 2) => {
                return Ok(Node::And(
                    Box::new(self.lower(&args[0])?),
                    Box::new(self.lower(&args[1])?),
                ));
            }
            ("_||_", false, 2) => {
                return Ok(Node::Or(
                    Box::new(self.lower(&args[0])?),
                    Box::new(self.lower(&args[1])?),
                ));
            }
            ("_?_:_", false, 3) => {
                return Ok(Node::Cond(
                    Box::new(self.lower(&args[0])?),
                    Box::new(self.lower(&args[1])?),
                    Box::new(self.lower(&args[2])?),
                ));
            }
            ("has", false, 1) => {
                return match &args[0].kind {
                    PKind::Select(operand, field) => {
                        let base = self.lower(operand)?;
                        let attr = match base {
                            Node::Attr(a) => a,
                            other => Box::new(Attr {
                                base: Base::Expr(other),
                                quals: Vec::new(),
                            }),
                        };
                        Ok(Node::Has(attr, Arc::from(field.as_str())))
                    }
                    _ => Err("invalid argument to has() macro".into()),
                };
            }
            _ => {}
        }
        if is_macro(name, target.is_some(), args) {
            return self.comprehension(name, target.expect("macro target"), args);
        }

        let member = target.is_some();
        let n = args.len();
        let f = match (name, member, n) {
            ("_+_", false, 2) => Func::Add,
            ("_-_", false, 2) => Func::Sub,
            ("_*_", false, 2) => Func::Mul,
            ("_/_", false, 2) => Func::Div,
            ("_%_", false, 2) => Func::Mod,
            ("-_", false, 1) => Func::Neg,
            ("!_", false, 1) => Func::Not,
            ("_==_", false, 2) => Func::Eq,
            ("_!=_", false, 2) => Func::Ne,
            ("_<_", false, 2) => Func::Lt,
            ("_<=_", false, 2) => Func::Le,
            ("_>_", false, 2) => Func::Gt,
            ("_>=_", false, 2) => Func::Ge,
            ("@in", false, 2) => Func::In,
            ("size", false, 1) | ("size", true, 0) => Func::Size,
            ("contains", true, 1) => Func::Contains,
            ("startsWith", true, 1) => Func::StartsWith,
            ("endsWith", true, 1) => Func::EndsWith,
            ("matches", false, 2) | ("matches", true, 1) => {
                let pat = &args[n - 1];
                if let PKind::Lit(Lit::Str(s)) = &pat.kind {
                    self.regexes.push(compile_regex(s));
                    Func::MatchesLit(self.regexes.len() - 1)
                } else {
                    Func::Matches
                }
            }
            ("int", false, 1) => Func::ToInt,
            ("uint", false, 1) => Func::ToUint,
            ("double", false, 1) => Func::ToDouble,
            ("string", false, 1) => Func::ToString,
            ("bool", false, 1) => Func::ToBool,
            ("bytes", false, 1) => Func::ToBytes,
            ("dyn", false, 1) => Func::Dyn,
            ("type", false, 1) => Func::TypeOf,
            ("timestamp", false, 1) => Func::Timestamp,
            ("duration", false, 1) => Func::Duration,
            ("getFullYear", true, 0 | 1) => Func::GetFullYear,
            ("getMonth", true, 0 | 1) => Func::GetMonth,
            ("getDayOfYear", true, 0 | 1) => Func::GetDayOfYear,
            ("getDayOfMonth", true, 0 | 1) => Func::GetDayOfMonth,
            ("getDate", true, 0 | 1) => Func::GetDate,
            ("getDayOfWeek", true, 0 | 1) => Func::GetDayOfWeek,
            ("getHours", true, 0 | 1) => Func::GetHours,
            ("getMinutes", true, 0 | 1) => Func::GetMinutes,
            ("getSeconds", true, 0 | 1) => Func::GetSeconds,
            ("getMilliseconds", true, 0 | 1) => Func::GetMilliseconds,
            ("lowercase", false, 1) => Func::Lowercase,
            ("uppercase", false, 1) => Func::Uppercase,
            ("b64decode", false, 1) => Func::B64Decode,
            ("json_parse", false, 1) => Func::JsonParse,
            ("date", false, 1) => Func::Date,
            ("now", false, 0) => Func::Now,
            ("now_plus", false, 1) => Func::NowPlus,
            ("now_minus", false, 1) => Func::NowMinus,
            _ => {
                let known = matches!(
                    name,
                    "size"
                        | "contains"
                        | "startsWith"
                        | "endsWith"
                        | "matches"
                        | "int"
                        | "uint"
                        | "double"
                        | "string"
                        | "bool"
                        | "bytes"
                        | "dyn"
                        | "type"
                        | "timestamp"
                        | "duration"
                        | "lowercase"
                        | "uppercase"
                        | "b64decode"
                        | "json_parse"
                        | "date"
                        | "now"
                        | "now_plus"
                        | "now_minus"
                        | "all"
                        | "exists"
                        | "exists_one"
                        | "existsOne"
                        | "map"
                        | "filter"
                        | "has"
                ) || name.starts_with("get");
                return Err(if known {
                    format!(
                        "found no matching overload for '{name}' with {n} argument(s){}",
                        if member { " (member call)" } else { "" }
                    )
                } else {
                    format!("undeclared reference to '{name}' (in container '')")
                });
            }
        };
        let mut lowered = Vec::with_capacity(n + usize::from(member));
        if let Some(t) = target {
            lowered.push(self.lower(t)?);
        }
        for a in args {
            lowered.push(self.lower(a)?);
        }
        Ok(Node::Call(f, lowered))
    }

    fn comprehension(&mut self, name: &str, target: &PExpr, args: &[PExpr]) -> Result<Node, String> {
        let var = match &args[0].kind {
            PKind::Ident(v) => v.clone(),
            _ => return Err("argument must be a simple name".into()),
        };
        let kind = match name {
            "all" => CompKind::All,
            "exists" => CompKind::Exists,
            "exists_one" | "existsOne" => CompKind::ExistsOne,
            "map" => CompKind::Map,
            _ => CompKind::Filter,
        };
        let range = self.lower(target)?;
        let slot = self.scope.len();
        self.scope.push(var);
        let res = (|| {
            let (filter, body) = if args.len() == 3 {
                (Some(self.lower(&args[1])?), self.lower(&args[2])?)
            } else {
                (None, self.lower(&args[1])?)
            };
            Ok::<_, String>((filter, body))
        })();
        self.scope.pop();
        let (filter, body) = res?;
        Ok(Node::Comp(Box::new(Comp {
            kind,
            range,
            slot,
            filter,
            body,
        })))
    }
}

/// Appends a qualifier to an attribute, or wraps a non-attribute expression in
/// a relative attribute.
fn push_qual(base: Node, q: Qual) -> Node {
    match base {
        Node::Attr(mut a) => {
            a.quals.push(q);
            Node::Attr(a)
        }
        other => Node::Attr(Box::new(Attr {
            base: Base::Expr(other),
            quals: vec![q],
        })),
    }
}

/// Mirrors Inngest's `parseUsedAttributes`: walks the parse tree through call
/// arguments (not member-call targets), comprehension ranges and select chains,
/// collecting `root.a.b` paths. At evaluation time, those that are absent from
/// the input make matching attributes evaluate to "unknown".
fn collect_patterns(p: &PExpr, run_roots: bool) -> Vec<Pattern> {
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<&PExpr> = vec![p];
    while let Some(e) = stack.pop() {
        match &e.kind {
            PKind::Call { name, target, args } => {
                if is_macro(name, target.is_some(), args) {
                    if let Some(t) = target {
                        stack.push(t);
                    }
                } else if name == "has" && target.is_none() && args.len() == 1 {
                    // has() is a test-only select in cel-go's AST.
                    add_select_path(&args[0], &mut out, &mut seen, run_roots);
                } else {
                    stack.extend(args.iter());
                }
            }
            PKind::Index(operand, index) => {
                // `_[_]` is a call in cel-go's AST: both args are walked.
                stack.push(operand);
                stack.push(index);
            }
            PKind::Select(..) => add_select_path(e, &mut out, &mut seen, run_roots),
            _ => {}
        }
    }
    out
}

fn add_select_path(e: &PExpr, out: &mut Vec<Pattern>, seen: &mut HashSet<String>, run_roots: bool) {
    let mut path: Vec<String> = Vec::new();
    let mut cur = e;
    while let PKind::Select(operand, field) = &cur.kind {
        path.push(field.clone());
        cur = operand;
    }
    path.reverse();
    match &cur.kind {
        PKind::Ident(name) => path.insert(0, name.clone()),
        PKind::Index(operand, _index) => {
            // Inngest's parser lifts string literals into `vars.*` before this
            // walk runs, so the index is never a string constant and its id is
            // always empty (this makes `event["data"].x` paths inert).
            let id = String::new();
            let name = match &operand.kind {
                PKind::Ident(n) => n.clone(),
                _ => String::new(),
            };
            path.insert(0, id);
            path.insert(0, name);
        }
        _ => {}
    }
    if path.len() < 2 {
        return;
    }
    let Some(root) = Root::from_name(&path[0], run_roots) else {
        return;
    };
    let key = path.join("\u{0}");
    if seen.insert(key) {
        out.push(Pattern {
            root,
            path: path[1..].iter().map(|s| Arc::from(s.as_str())).collect(),
        });
    }
}

/// Inngest's default restrictive validation policy (used for `cancelOn.if`):
/// no macros, and only logic/comparison operators, indexing, `in`, and
/// int/uint/double conversions. Like Go, list and map literals are not
/// inspected.
pub(crate) fn validate_restrictive(n: &Node, errs: &mut Vec<String>) {
    match n {
        Node::Const(_) | Node::List(_) | Node::Map(_) => {}
        Node::Comp(c) => {
            let name = match c.kind {
                CompKind::All => "all",
                CompKind::Exists => "exists",
                CompKind::ExistsOne => "exists_one",
                CompKind::Map => "map",
                CompKind::Filter => "filter",
            };
            errs.push(format!("macros are disallowed (uses '{name}')"));
        }
        Node::Has(a, _) => {
            errs.push("macros are disallowed (uses 'has')".into());
            validate_attr(a, errs);
        }
        Node::Attr(a) => validate_attr(a, errs),
        Node::And(a, b) | Node::Or(a, b) => {
            validate_restrictive(a, errs);
            validate_restrictive(b, errs);
        }
        Node::Cond(a, b, c) => {
            validate_restrictive(a, errs);
            validate_restrictive(b, errs);
            validate_restrictive(c, errs);
        }
        Node::Call(f, args) => {
            if !f.restrictive_allowed() {
                errs.push(format!("function calls are disallowed (uses '{}')", f.name()));
            }
            for a in args {
                validate_restrictive(a, errs);
            }
        }
    }
}

fn validate_attr(a: &Attr, errs: &mut Vec<String>) {
    if let Base::Expr(e) = &a.base {
        validate_restrictive(e, errs);
    }
    for q in &a.quals {
        if let Qual::Dyn(e) = q {
            validate_restrictive(e, errs);
        }
    }
}

/// Whether the expression references the `event` variable anywhere.
pub(crate) fn references_root(n: &Node, root: Root) -> bool {
    match n {
        Node::Const(_) => false,
        Node::Attr(a) | Node::Has(a, _) => attr_references(a, root),
        Node::List(items) => items.iter().any(|i| references_root(i, root)),
        Node::Map(entries) => entries
            .iter()
            .any(|(k, v)| references_root(k, root) || references_root(v, root)),
        Node::And(a, b) | Node::Or(a, b) => references_root(a, root) || references_root(b, root),
        Node::Cond(a, b, c) => {
            references_root(a, root) || references_root(b, root) || references_root(c, root)
        }
        Node::Call(_, args) => args.iter().any(|a| references_root(a, root)),
        Node::Comp(c) => {
            references_root(&c.range, root)
                || c.filter.as_ref().is_some_and(|f| references_root(f, root))
                || references_root(&c.body, root)
        }
    }
}

fn attr_references(a: &Attr, root: Root) -> bool {
    let base = match &a.base {
        Base::Root(r) => *r == root,
        Base::Local(_) => false,
        Base::Expr(e) => references_root(e, root),
    };
    base || a.quals.iter().any(|q| matches!(q, Qual::Dyn(e) if references_root(e, root)))
}

/// The trigger event names an expression can match, as worked out by [`event_name_set`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NameSet {
    pub names: std::collections::BTreeSet<String>,
    /// The expression is true exactly when `event.name` is in `names`.
    pub exact: bool,
}

/// If evaluating `n` to `true` requires `event.name` to equal one of a finite set of string
/// literals, returns that set. Recognizes `event.name == "x"` (either side), `event.name in
/// ["x", ...]`, and `&&`/`||` over those. `exact` is set when nothing else is involved, so
/// checking the name alone gives the same answer as evaluating the expression.
pub(crate) fn event_name_set(n: &Node) -> Option<NameSet> {
    match n {
        Node::Call(Func::Eq, args) if args.len() == 2 => {
            let lit = match (&args[0], &args[1]) {
                (a, Node::Const(Const::Str(s))) | (Node::Const(Const::Str(s)), a) if is_event_name(a) => s,
                _ => return None,
            };
            Some(NameSet {
                names: [lit.to_string()].into(),
                exact: true,
            })
        }
        Node::Call(Func::In, args) if args.len() == 2 && is_event_name(&args[0]) => {
            let Node::List(items) = &args[1] else {
                return None;
            };
            let names = items
                .iter()
                .map(|i| match i {
                    Node::Const(Const::Str(s)) => Some(s.to_string()),
                    _ => None,
                })
                .collect::<Option<_>>()?;
            Some(NameSet { names, exact: true })
        }
        // A true `a && b` needs both sides true; a true `a || b` needs one of them true.
        Node::And(a, b) => match (event_name_set(a), event_name_set(b)) {
            (Some(x), Some(y)) => Some(NameSet {
                names: x.names.intersection(&y.names).cloned().collect(),
                exact: x.exact && y.exact,
            }),
            (Some(x), None) | (None, Some(x)) => Some(NameSet {
                names: x.names,
                exact: false,
            }),
            (None, None) => None,
        },
        Node::Or(a, b) => {
            let (x, y) = (event_name_set(a)?, event_name_set(b)?);
            Some(NameSet {
                names: x.names.union(&y.names).cloned().collect(),
                exact: x.exact && y.exact,
            })
        }
        _ => None,
    }
}

/// `event.name` or `event["name"]`.
fn is_event_name(n: &Node) -> bool {
    let Node::Attr(a) = n else { return false };
    matches!(a.base, Base::Root(Root::Event))
        && a.quals.len() == 1
        && match &a.quals[0] {
            Qual::Field(f) => &**f == "name",
            Qual::Const(Const::Str(s)) => &**s == "name",
            _ => false,
        }
}
