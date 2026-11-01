use serde_json::json;

use super::*;
use crate::value::go_float;

/// Maps an evaluation result to Go's `(%T, %v, err?)` as printed by the
/// reference oracle (`expressions.Evaluate` on the Go implementation).
fn go_repr(e: &Expression, data: &Json) -> (String, String, bool) {
    let vars = Vars {
        event: data.get("event"),
        async_: data.get("async"),
    };
    let v = e.run(vars);
    let (t, s) = match &v {
        Val::Unknown => ("bool", "false".to_string()),
        Val::Err(_) => return ("bool".into(), "false".into(), true),
        Val::Bool(b) => ("bool", b.to_string()),
        Val::Int(i) => ("int64", i.to_string()),
        Val::Uint(u) => ("uint64", u.to_string()),
        Val::Double(d) => ("float64", go_float(*d)),
        Val::Str(s) => ("string", s.as_str().to_string()),
        Val::Ts(t) => ("time.Time", timefmt::rfc3339_nano(t)),
        Val::Dur(d) => ("time.Duration", timefmt::go_duration(*d)),
        Val::Null => ("structpb.NullValue", "NULL_VALUE".to_string()),
        Val::Type(k) => ("string", k.name().to_string()),
        Val::List(_) => ("list", v.go_string()),
        Val::Map(_) => ("map", v.go_string()),
        Val::Bytes(_) => ("[]uint8", v.go_string()),
    };
    (t.to_string(), s, false)
}

fn normalize_go_type(t: &str) -> &str {
    if t.starts_with("map[") {
        "map"
    } else if t.starts_with('[') && t != "[]uint8" {
        "list"
    } else {
        t
    }
}

/// Compares against Go oracle output lines. Returns (hard mismatches, soft
/// mismatches (container formatting only), total).
fn diff_against_oracle(lines: &str, verbose: bool) -> (Vec<String>, Vec<String>, usize) {
    let mut hard = Vec::new();
    let mut soft = Vec::new();
    let mut total = 0;
    for line in lines.lines().filter(|l| !l.trim().is_empty()) {
        let rec: Json = serde_json::from_str(line).expect("oracle line");
        let expr = rec["expr"].as_str().unwrap_or_default();
        let data = &rec["data"];
        total += 1;
        let go_err = rec.get("err").is_some() || rec.get("panic").is_some();
        let go_t = normalize_go_type(rec["type"].as_str().unwrap_or_default()).to_string();
        let go_v = rec["v"].as_str().unwrap_or_default().to_string();
        let ctx = format!("{expr}  [{}]", rec["dataset"].as_str().unwrap_or("?"));
        let compiled = Expression::compile(expr);
        let (t, v, err) = match &compiled {
            Ok(e) => go_repr(e, data),
            Err(_) => ("bool".into(), "false".into(), true),
        };
        if go_err != err {
            hard.push(format!(
                "{ctx}\n    go: err={go_err} {go_t} {go_v:?} {}\n    rs: err={err} {t} {v:?} {}",
                rec.get("err").and_then(Json::as_str).unwrap_or(""),
                match (&compiled, err) {
                    (Err(e), _) => e.to_string(),
                    (Ok(e), true) => format!("{:?}", e.eval(Vars { event: data.get("event"), async_: data.get("async") })),
                    _ => String::new(),
                }
            ));
            continue;
        }
        if !go_err && (go_t != t || go_v != v) {
            let msg = format!("{ctx}\n    go: {go_t} {go_v:?}\n    rs: {t} {v:?}");
            if (go_t == "list" || go_t == "map") && go_t == t {
                soft.push(msg);
            } else {
                hard.push(msg);
            }
            continue;
        }
        // Restrictive validation (only meaningful when Go's type-checking
        // compile succeeded).
        if let (Ok(e), Some(go_ok)) = (&compiled, rec.get("restrictive").and_then(Json::as_bool))
            && rec.get("checkerr").is_none()
            && e.validate_restrictive().is_ok() != go_ok
        {
            hard.push(format!("{ctx}\n    restrictive: go={go_ok} rs={:?}", e.validate_restrictive()));
        }
    }
    if verbose {
        for h in &hard {
            eprintln!("HARD {h}");
        }
        for s in &soft {
            eprintln!("SOFT {s}");
        }
        eprintln!("total={total} hard={} soft={}", hard.len(), soft.len());
    }
    (hard, soft, total)
}

#[test]
fn differential_against_go_oracle_env() {
    let Ok(path) = std::env::var("OXIDLE_EXPR_ORACLE") else {
        return;
    };
    let lines = std::fs::read_to_string(path).expect("read oracle output");
    let (hard, _, _) = diff_against_oracle(&lines, true);
    assert!(hard.is_empty(), "{} mismatches", hard.len());
}
