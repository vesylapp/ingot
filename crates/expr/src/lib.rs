//! Inngest-compatible CEL expression engine.
//!
//! Inngest evaluates trigger expressions, `waitForEvent`/`cancelOn` matching
//! and flow-control key expressions with cel-go plus a set of custom
//! semantics (missing attributes behave like "unknown" ≈ null, JS-like
//! truthiness for `&&`/`||`/`?:`, string concatenation for mismatched `+`,
//! cross-type numeric comparisons, custom functions, ...). This crate
//! reimplements those semantics with its own parser and a tree-walking
//! evaluator over a compiled IR.
//!
//! Expressions are compiled once ([`Expression::compile`], or cached through
//! [`ExprCache`]) and evaluated many times against borrowed JSON input
//! without copying it.

mod compile;
mod eval;
mod parser;
mod timefmt;
mod value;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

pub use serde_json::Value as Json;

use crate::compile::{Program, Root};
use crate::eval::Evaluator;
use crate::value::Val;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ExprError {
    #[error("compile error: {0}")]
    Compile(String),
    #[error("evaluation error: {0}")]
    Eval(String),
    #[error("{0}")]
    Type(String),
    #[error("invalid expression: {0}")]
    Policy(String),
}

/// Variables bound during evaluation. `event` and `async` are JSON objects (Inngest event maps:
/// {name,data,user,id,ts,v?}); either may be absent.
#[derive(Default, Clone, Copy)]
pub struct Vars<'a> {
    pub event: Option<&'a Json>,
    pub async_: Option<&'a Json>,
}

struct Inner {
    source: String,
    program: Program,
    references_event: bool,
}

/// A compiled expression. Cheap to clone; safe to share across threads.
#[derive(Clone)]
pub struct Expression {
    inner: Arc<Inner>,
}

impl fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expression")
            .field("source", &self.inner.source)
            .finish()
    }
}

impl Expression {
    /// Parses and compiles an expression. Fails on syntax errors, references to
    /// undeclared identifiers (only `event`, `async` and `vars` are declared),
    /// and unknown functions / wrong arities.
    pub fn compile(src: &str) -> Result<Expression, ExprError> {
        let parsed = parser::parse(src).map_err(ExprError::Compile)?;
        let program = compile::compile(&parsed).map_err(ExprError::Compile)?;
        let references_event = compile::references_root(&program.root, Root::Event);
        Ok(Expression {
            inner: Arc::new(Inner {
                source: src.to_string(),
                program,
                references_event,
            }),
        })
    }

    pub fn source(&self) -> &str {
        &self.inner.source
    }

    /// Restrictive policy used for cancelOn `if`: no macros; only ?:, &&, ||, !, comparisons,
    /// index/select, in, int(), uint(), double().
    pub fn validate_restrictive(&self) -> Result<(), ExprError> {
        let mut errs = Vec::new();
        compile::validate_restrictive(&self.inner.program.root, &mut errs);
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ExprError::Policy(errs.join("; ")))
        }
    }

    fn run<'s>(&'s self, vars: Vars<'s>) -> Val<'s> {
        Evaluator::new(&self.inner.program, vars.event, vars.async_).run()
    }

    /// Evaluate and convert the result to JSON (timestamps -> RFC3339 strings, durations ->
    /// Go-style strings like "1h0m0s", bytes -> base64 strings, unknown -> null, ints -> JSON
    /// ints, doubles -> JSON floats).
    pub fn eval(&self, vars: Vars<'_>) -> Result<Json, ExprError> {
        match self.run(vars) {
            Val::Err(e) => Err(ExprError::Eval(e.to_string())),
            v => v.to_json().map_err(ExprError::Eval),
        }
    }

    /// Boolean contexts (triggers, if-matching). Non-bool result => Err(Type). Whole-result
    /// unknown => Ok(false).
    pub fn eval_bool(&self, vars: Vars<'_>) -> Result<bool, ExprError> {
        match self.run(vars) {
            Val::Bool(b) => Ok(b),
            Val::Unknown => Ok(false),
            Val::Err(e) => Err(ExprError::Eval(e.to_string())),
            other => Err(ExprError::Type(format!(
                "expression did not return true or false: returned type {} ({})",
                other.kind().name(),
                other.go_string()
            ))),
        }
    }

    /// Key expressions (concurrency/throttle/rateLimit/debounce/batch/singleton keys): strings
    /// are returned as-is, everything else is formatted like Go's `%v`; null/unknown => "<nil>".
    pub fn eval_key(&self, vars: Vars<'_>) -> Result<String, ExprError> {
        key_string(self.run(vars))
    }

    /// Like [`Expression::eval_key`], but returns `Ok(None)` when the result is boolean
    /// `false` (or the whole result is unknown, which Inngest also evaluates to `false`).
    pub fn eval_key_opt(&self, vars: Vars<'_>) -> Result<Option<String>, ExprError> {
        match self.run(vars) {
            Val::Bool(false) | Val::Unknown => Ok(None),
            v => key_string(v).map(Some),
        }
    }

    /// Priority: the result must be numeric; doubles with an integral value are accepted.
    pub fn eval_int(&self, vars: Vars<'_>) -> Result<i64, ExprError> {
        match self.run(vars) {
            Val::Int(i) => Ok(i),
            Val::Uint(u) => i64::try_from(u)
                .map_err(|_| ExprError::Type(format!("expression returned an out of range int: {u}"))),
            Val::Double(d)
                if d.is_finite()
                    && d.fract() == 0.0
                    && d >= -9.223_372_036_854_775_808e18
                    && d < 9.223_372_036_854_775_808e18 =>
            {
                Ok(d as i64)
            }
            Val::Err(e) => Err(ExprError::Eval(e.to_string())),
            Val::Unknown => Err(ExprError::Type("expression returned non-int: false".into())),
            other => Err(ExprError::Type(format!(
                "expression returned non-int: {}",
                other.go_string()
            ))),
        }
    }

    /// True if the expression references the `event` variable anywhere.
    pub fn references_event(&self) -> bool {
        self.inner.references_event
    }
}

fn key_string(v: Val<'_>) -> Result<String, ExprError> {
    match v {
        Val::Str(s) => Ok(s.as_str().to_string()),
        Val::Null | Val::Unknown => Ok("<nil>".to_string()),
        Val::Err(e) => Err(ExprError::Eval(e.to_string())),
        other => Ok(other.go_string()),
    }
}

/// Thread-safe compile cache keyed by source string (bounded; arbitrary entries are evicted
/// when full).
pub struct ExprCache {
    capacity: usize,
    map: RwLock<HashMap<String, Expression>>,
}

impl ExprCache {
    pub fn new(capacity: usize) -> Self {
        ExprCache {
            capacity,
            map: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, src: &str) -> Result<Expression, ExprError> {
        if let Ok(m) = self.map.read()
            && let Some(e) = m.get(src)
        {
            return Ok(e.clone());
        }
        let expr = Expression::compile(src)?;
        if self.capacity == 0 {
            return Ok(expr);
        }
        let mut m = match self.map.write() {
            Ok(m) => m,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(existing) = m.get(src) {
            return Ok(existing.clone());
        }
        if m.len() >= self.capacity {
            // Evict ~1/8th of the entries (at least one); HashMap iteration order is
            // effectively random.
            let n = (self.capacity / 8).max(1);
            let victims: Vec<String> = m.keys().take(n).cloned().collect();
            for k in victims {
                m.remove(&k);
            }
        }
        m.insert(src.to_string(), expr.clone());
        Ok(expr)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.read().map(|m| m.len()).unwrap_or(0)
    }
}

impl Default for ExprCache {
    fn default() -> Self {
        ExprCache::new(10_000)
    }
}
