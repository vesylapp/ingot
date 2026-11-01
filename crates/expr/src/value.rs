//! Runtime values. Values borrow from the input JSON and the compiled
//! expression where possible (`'a`), and use `Rc` for owned data so cloning is
//! always cheap.

use std::cmp::Ordering;
use std::fmt::Write as _;
use std::rc::Rc;

use serde_json::{Map, Value as Json};

use crate::timefmt;

pub(crate) const GO_NIL_PREFIX: &str = "\u{0}nil:";

/// Converts an error value's message for the public API.
pub(crate) fn err_message(e: &str) -> String {
    match e.strip_prefix(GO_NIL_PREFIX) {
        Some(reason) => format!("expression did not return true or false ({reason})"),
        None => e.to_string(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Null,
    Bool,
    Int,
    Uint,
    Double,
    String,
    Bytes,
    List,
    Map,
    Timestamp,
    Duration,
    Type,
    Unknown,
    Err,
}

impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Kind::Null => "null_type",
            Kind::Bool => "bool",
            Kind::Int => "int",
            Kind::Uint => "uint",
            Kind::Double => "double",
            Kind::String => "string",
            Kind::Bytes => "bytes",
            Kind::List => "list",
            Kind::Map => "map",
            Kind::Timestamp => "google.protobuf.Timestamp",
            Kind::Duration => "google.protobuf.Duration",
            Kind::Type => "type",
            Kind::Unknown => "unknown",
            Kind::Err => "error",
        }
    }
}

/// A string that is either borrowed (from input JSON or the expression) or owned.
#[derive(Clone, Debug)]
pub(crate) enum Str<'a> {
    B(&'a str),
    O(Rc<str>),
}

impl Str<'_> {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Str::B(s) => s,
            Str::O(s) => s,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Bytes<'a> {
    B(&'a [u8]),
    O(Rc<[u8]>),
}

impl Bytes<'_> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Bytes::B(s) => s,
            Bytes::O(s) => s,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum List<'a> {
    Json(&'a [Json]),
    Owned(Rc<Vec<Val<'a>>>),
}

impl<'a> List<'a> {
    pub(crate) fn len(&self) -> usize {
        match self {
            List::Json(l) => l.len(),
            List::Owned(l) => l.len(),
        }
    }

    pub(crate) fn get(&self, i: usize) -> Option<Val<'a>> {
        match self {
            List::Json(l) => l.get(i).map(Val::from_json),
            List::Owned(l) => l.get(i).cloned(),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Val<'a>> + '_ {
        (0..self.len()).filter_map(move |i| self.get(i))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum MapV<'a> {
    Json(&'a Map<String, Json>),
    Owned(Rc<Vec<(Val<'a>, Val<'a>)>>),
}

impl<'a> MapV<'a> {
    pub(crate) fn len(&self) -> usize {
        match self {
            MapV::Json(m) => m.len(),
            MapV::Owned(m) => m.len(),
        }
    }

    pub(crate) fn get_str(&self, k: &str) -> Option<Val<'a>> {
        match self {
            MapV::Json(m) => m.get(k).map(Val::from_json),
            MapV::Owned(m) => m
                .iter()
                .find(|(key, _)| matches!(key, Val::Str(s) if s.as_str() == k))
                .map(|(_, v)| v.clone()),
        }
    }

    /// Looks up a key using CEL's heterogeneous key semantics. `Err` for key
    /// types that can never be map keys.
    pub(crate) fn get(&self, k: &Val<'_>) -> Result<Option<Val<'a>>, Rc<str>> {
        match k {
            Val::Str(s) => Ok(self.get_str(s.as_str())),
            Val::Int(_) | Val::Uint(_) | Val::Double(_) | Val::Bool(_) => match self {
                MapV::Json(_) => Ok(None),
                MapV::Owned(m) => Ok(m
                    .iter()
                    .find(|(key, _)| key_eq(key, k))
                    .map(|(_, v)| v.clone())),
            },
            other => Err(format!("unsupported key type: {}", other.kind().name()).into()),
        }
    }

    pub(crate) fn keys(&self) -> Vec<Val<'a>> {
        match self {
            MapV::Json(m) => m.keys().map(|k| Val::Str(Str::B(k.as_str()))).collect(),
            MapV::Owned(m) => m.iter().map(|(k, _)| k.clone()).collect(),
        }
    }

    pub(crate) fn entries(&self) -> Vec<(Val<'a>, Val<'a>)> {
        match self {
            MapV::Json(m) => m
                .iter()
                .map(|(k, v)| (Val::Str(Str::B(k.as_str())), Val::from_json(v)))
                .collect(),
            MapV::Owned(m) => m.as_ref().clone(),
        }
    }
}

fn key_eq(a: &Val<'_>, b: &Val<'_>) -> bool {
    match (a, b) {
        (Val::Bool(x), Val::Bool(y)) => x == y,
        (Val::Str(x), Val::Str(y)) => x.as_str() == y.as_str(),
        _ => num_cmp(a, b) == Some(Ordering::Equal),
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Val<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    Str(Str<'a>),
    Bytes(Bytes<'a>),
    List(List<'a>),
    Map(MapV<'a>),
    Ts(timefmt::Ts),
    /// Nanoseconds (Go's time.Duration).
    Dur(i64),
    Type(Kind),
    Unknown,
    Err(Rc<str>),
}

impl<'a> Val<'a> {
    pub(crate) fn err(msg: impl Into<String>) -> Val<'a> {
        Val::Err(Rc::from(msg.into()))
    }

    pub(crate) fn no_overload(name: &str) -> Val<'a> {
        Val::err(format!("no such overload: {name}"))
    }

    /// A Go `nil` result (e.g. a failed `date()`): it is falsy in logical
    /// contexts, but aborts evaluation when used as a function argument or
    /// returned.
    pub(crate) fn go_nil(reason: &str) -> Val<'a> {
        Val::err(format!("{GO_NIL_PREFIX}{reason}"))
    }

    pub(crate) fn is_go_nil(&self) -> bool {
        matches!(self, Val::Err(e) if e.starts_with(GO_NIL_PREFIX))
    }

    pub(crate) fn string(s: String) -> Val<'a> {
        Val::Str(Str::O(Rc::from(s)))
    }

    pub(crate) fn from_json(j: &'a Json) -> Val<'a> {
        match j {
            Json::Null => Val::Null,
            Json::Bool(b) => Val::Bool(*b),
            // JSON numbers are always doubles, as in Go's encoding/json.
            Json::Number(n) => Val::Double(n.as_f64().unwrap_or(f64::NAN)),
            Json::String(s) => Val::Str(Str::B(s.as_str())),
            Json::Array(a) => Val::List(List::Json(a.as_slice())),
            Json::Object(m) => Val::Map(MapV::Json(m)),
        }
    }

    /// Converts an owned JSON value into a fully owned value.
    pub(crate) fn from_json_owned(j: &Json) -> Val<'a> {
        match j {
            Json::Null => Val::Null,
            Json::Bool(b) => Val::Bool(*b),
            Json::Number(n) => Val::Double(n.as_f64().unwrap_or(f64::NAN)),
            Json::String(s) => Val::Str(Str::O(Rc::from(s.as_str()))),
            Json::Array(a) => Val::List(List::Owned(Rc::new(
                a.iter().map(Val::from_json_owned).collect(),
            ))),
            Json::Object(m) => Val::Map(MapV::Owned(Rc::new(
                m.iter()
                    .map(|(k, v)| (Val::Str(Str::O(Rc::from(k.as_str()))), Val::from_json_owned(v)))
                    .collect(),
            ))),
        }
    }

    pub(crate) fn kind(&self) -> Kind {
        match self {
            Val::Null => Kind::Null,
            Val::Bool(_) => Kind::Bool,
            Val::Int(_) => Kind::Int,
            Val::Uint(_) => Kind::Uint,
            Val::Double(_) => Kind::Double,
            Val::Str(_) => Kind::String,
            Val::Bytes(_) => Kind::Bytes,
            Val::List(_) => Kind::List,
            Val::Map(_) => Kind::Map,
            Val::Ts(_) => Kind::Timestamp,
            Val::Dur(_) => Kind::Duration,
            Val::Type(_) => Kind::Type,
            Val::Unknown => Kind::Unknown,
            Val::Err(_) => Kind::Err,
        }
    }

    pub(crate) fn is_unknown_or_err(&self) -> bool {
        matches!(self, Val::Unknown | Val::Err(_))
    }

    /// JS-like truthiness, as implemented by Inngest's `isTruthy`.
    pub(crate) fn truthy(&self) -> bool {
        match self {
            Val::Unknown | Val::Err(_) | Val::Null => false,
            Val::Bool(b) => *b,
            Val::Str(s) => !s.as_str().is_empty(),
            Val::Int(i) => *i != 0,
            Val::Uint(u) => *u != 0,
            Val::Double(d) => *d != 0.0,
            _ => true,
        }
    }

    /// Converts the value to JSON for the public API.
    pub(crate) fn to_json(&self) -> Result<Json, String> {
        Ok(match self {
            Val::Null | Val::Unknown => Json::Null,
            Val::Bool(b) => Json::Bool(*b),
            Val::Int(i) => Json::from(*i),
            Val::Uint(u) => Json::from(*u),
            Val::Double(d) => serde_json::Number::from_f64(*d).map_or(Json::Null, Json::Number),
            Val::Str(s) => Json::String(s.as_str().to_string()),
            Val::Bytes(b) => {
                use base64::Engine as _;
                Json::String(base64::engine::general_purpose::STANDARD.encode(b.as_slice()))
            }
            Val::List(List::Json(l)) => Json::Array(l.to_vec()),
            Val::List(List::Owned(l)) => {
                Json::Array(l.iter().map(|v| v.to_json()).collect::<Result<_, _>>()?)
            }
            Val::Map(MapV::Json(m)) => Json::Object((*m).clone()),
            Val::Map(MapV::Owned(m)) => {
                let mut out = Map::new();
                for (k, v) in m.iter() {
                    let key = match k {
                        Val::Str(s) => s.as_str().to_string(),
                        other => other.go_string(),
                    };
                    out.insert(key, v.to_json()?);
                }
                Json::Object(out)
            }
            Val::Ts(t) => Json::String(timefmt::rfc3339_nano(t)),
            Val::Dur(d) => Json::String(timefmt::go_duration(*d)),
            Val::Type(k) => Json::String(k.name().to_string()),
            Val::Err(e) => return Err(e.to_string()),
        })
    }

    /// Go `fmt.Sprintf("%v", v.Value())` formatting of a CEL value.
    pub(crate) fn go_string(&self) -> String {
        let mut s = String::new();
        self.go_fmt(&mut s, true);
        s
    }

    fn go_fmt(&self, out: &mut String, top: bool) {
        match self {
            // A top-level CEL null's Go value is structpb.NullValue_NULL_VALUE;
            // nulls nested in native containers are Go nils.
            Val::Null => out.push_str(if top { "NULL_VALUE" } else { "<nil>" }),
            Val::Unknown => out.push_str("<nil>"),
            Val::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Val::Int(i) => {
                let _ = write!(out, "{i}");
            }
            Val::Uint(u) => {
                let _ = write!(out, "{u}");
            }
            Val::Double(d) => out.push_str(&go_float(*d)),
            Val::Str(s) => out.push_str(s.as_str()),
            Val::Bytes(b) => {
                out.push('[');
                for (i, byte) in b.as_slice().iter().enumerate() {
                    if i > 0 {
                        out.push(' ');
                    }
                    let _ = write!(out, "{byte}");
                }
                out.push(']');
            }
            Val::List(l) => {
                out.push('[');
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        out.push(' ');
                    }
                    v.go_fmt(out, false);
                }
                out.push(']');
            }
            Val::Map(m) => {
                // Go's fmt prints maps with sorted keys.
                let mut entries: Vec<(String, Val<'_>)> = m
                    .entries()
                    .into_iter()
                    .map(|(k, v)| {
                        let mut ks = String::new();
                        k.go_fmt(&mut ks, false);
                        (ks, v)
                    })
                    .collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                out.push_str("map[");
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        out.push(' ');
                    }
                    out.push_str(k);
                    out.push(':');
                    v.go_fmt(out, false);
                }
                out.push(']');
            }
            Val::Ts(t) => out.push_str(&timefmt::go_time(t)),
            Val::Dur(d) => out.push_str(&timefmt::go_duration(*d)),
            Val::Type(k) => out.push_str(k.name()),
            Val::Err(e) => out.push_str(e),
        }
    }
}

/// Formats a float like Go's `%v` (strconv 'g' with shortest precision).
pub(crate) fn go_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".into() } else { "-Inf".into() };
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    // Rust's `{:e}` yields the shortest round-tripping digits.
    let sci = format!("{:e}", f.abs());
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let neg = if f < 0.0 { "-" } else { "" };
    // Go uses %e when exp < -4 || exp >= eprec, with eprec = 6 for shortest.
    if !(-4..6).contains(&exp) {
        // %e form: d.ddde±XX (at least two exponent digits).
        let mut s = String::from(neg);
        s.push_str(&digits[..1]);
        if digits.len() > 1 {
            s.push('.');
            s.push_str(&digits[1..]);
        }
        let _ = write!(s, "e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs());
        return s;
    }
    let mut s = String::from(neg);
    if exp < 0 {
        s.push_str("0.");
        for _ in 0..(-exp - 1) {
            s.push('0');
        }
        s.push_str(&digits);
    } else {
        let int_len = (exp + 1) as usize;
        if digits.len() <= int_len {
            s.push_str(&digits);
            for _ in digits.len()..int_len {
                s.push('0');
            }
        } else {
            s.push_str(&digits[..int_len]);
            s.push('.');
            s.push_str(&digits[int_len..]);
        }
    }
    s
}

/// Numeric comparison across int/uint/double, mirroring cel-go. `None` if
/// either side isn't numeric or a NaN is involved.
pub(crate) fn num_cmp(a: &Val<'_>, b: &Val<'_>) -> Option<Ordering> {
    match (a, b) {
        (Val::Int(x), Val::Int(y)) => Some(x.cmp(y)),
        (Val::Uint(x), Val::Uint(y)) => Some(x.cmp(y)),
        (Val::Double(x), Val::Double(y)) => x.partial_cmp(y),
        (Val::Int(x), Val::Uint(y)) => Some(cmp_int_uint(*x, *y)),
        (Val::Uint(x), Val::Int(y)) => Some(cmp_int_uint(*y, *x).reverse()),
        (Val::Int(x), Val::Double(y)) => cmp_int_double(*x, *y),
        (Val::Double(x), Val::Int(y)) => cmp_int_double(*y, *x).map(Ordering::reverse),
        (Val::Uint(x), Val::Double(y)) => cmp_uint_double(*x, *y),
        (Val::Double(x), Val::Uint(y)) => cmp_uint_double(*y, *x).map(Ordering::reverse),
        _ => None,
    }
}

fn cmp_int_uint(i: i64, u: u64) -> Ordering {
    if i < 0 {
        Ordering::Less
    } else {
        (i as u64).cmp(&u)
    }
}

fn cmp_int_double(i: i64, d: f64) -> Option<Ordering> {
    if d.is_nan() {
        return None;
    }
    if d < i64::MIN as f64 {
        return Some(Ordering::Greater);
    }
    if d > i64::MAX as f64 {
        return Some(Ordering::Less);
    }
    (i as f64).partial_cmp(&d)
}

fn cmp_uint_double(u: u64, d: f64) -> Option<Ordering> {
    if d.is_nan() {
        return None;
    }
    if d < 0.0 {
        return Some(Ordering::Greater);
    }
    if d > u64::MAX as f64 {
        return Some(Ordering::Less);
    }
    (u as f64).partial_cmp(&d)
}

fn is_num(v: &Val<'_>) -> bool {
    matches!(v, Val::Int(_) | Val::Uint(_) | Val::Double(_))
}

/// CEL heterogeneous equality (cel-go `types.Equal`).
pub(crate) fn cel_eq(a: &Val<'_>, b: &Val<'_>) -> bool {
    if is_num(a) && is_num(b) {
        return num_cmp(a, b) == Some(Ordering::Equal);
    }
    match (a, b) {
        (Val::Null, Val::Null) => true,
        (Val::Bool(x), Val::Bool(y)) => x == y,
        (Val::Str(x), Val::Str(y)) => x.as_str() == y.as_str(),
        (Val::Bytes(x), Val::Bytes(y)) => x.as_slice() == y.as_slice(),
        (Val::Ts(x), Val::Ts(y)) => x == y,
        (Val::Dur(x), Val::Dur(y)) => x == y,
        (Val::Type(x), Val::Type(y)) => x == y,
        (Val::List(x), Val::List(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| cel_eq(&p, &q))
        }
        (Val::Map(x), Val::Map(y)) => {
            if x.len() != y.len() {
                return false;
            }
            x.entries().iter().all(|(k, v)| match y.get(k) {
                Ok(Some(w)) => cel_eq(v, &w),
                _ => false,
            })
        }
        _ => false,
    }
}

/// CEL ordering (cel-go `Compare`). `Err` value when not comparable.
pub(crate) fn cel_cmp<'a>(a: &Val<'_>, b: &Val<'_>) -> Result<Ordering, Val<'a>> {
    if is_num(a) && is_num(b) {
        return num_cmp(a, b).ok_or_else(|| Val::err("NaN values cannot be ordered"));
    }
    match (a, b) {
        (Val::Str(x), Val::Str(y)) => Ok(x.as_str().cmp(y.as_str())),
        (Val::Bytes(x), Val::Bytes(y)) => Ok(x.as_slice().cmp(y.as_slice())),
        (Val::Bool(x), Val::Bool(y)) => Ok(x.cmp(y)),
        (Val::Ts(x), Val::Ts(y)) => Ok(x.cmp(y)),
        (Val::Dur(x), Val::Dur(y)) => Ok(x.cmp(y)),
        _ => Err(Val::err("no such overload")),
    }
}
