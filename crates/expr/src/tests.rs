use serde_json::json;

use super::*;
use crate::value::go_float;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ev(data: Json) -> Json {
    json!({"name": "app/test", "id": "01H", "ts": 1774512121538.0, "user": {}, "data": data})
}

fn run_with(src: &str, event: Option<&Json>, async_: Option<&Json>) -> Result<Json, ExprError> {
    Expression::compile(src)?.eval(Vars { event, async_ })
}

/// Evaluates with `event.data = data`.
fn eval_data(src: &str, data: Json) -> Json {
    let e = ev(data);
    run_with(src, Some(&e), None).unwrap_or_else(|err| panic!("{src}: {err}"))
}

fn bool_data(src: &str, data: Json) -> bool {
    let e = ev(data);
    Expression::compile(src)
        .unwrap_or_else(|err| panic!("{src}: {err}"))
        .eval_bool(Vars {
            event: Some(&e),
            async_: None,
        })
        .unwrap_or_else(|err| panic!("{src}: {err}"))
}

fn eval_err(src: &str, data: Json) -> ExprError {
    let e = ev(data);
    match run_with(src, Some(&e), None) {
        Ok(v) => panic!("{src}: expected error, got {v}"),
        Err(e) => e,
    }
}

fn key(src: &str, data: Json) -> String {
    let e = ev(data);
    Expression::compile(src)
        .unwrap()
        .eval_key(Vars {
            event: Some(&e),
            async_: None,
        })
        .unwrap_or_else(|err| panic!("{src}: {err}"))
}

/// Maps an evaluation result to Go's `(%T, %v, err?)` as printed by the
/// reference oracle (`expressions.Evaluate` in the Go implementation).
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
        Val::Ts(t) => ("time.Time", timefmt::rfc3339_nano(&t.with_timezone(&chrono::Utc).fixed_offset())),
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

// ---------------------------------------------------------------------------
// Differential test against the Go implementation
// ---------------------------------------------------------------------------

/// `tests/fixtures/go_oracle.jsonl` holds results produced by running Inngest's
/// Go `expressions.Evaluate` / `expressions.Validate` over a corpus of
/// expressions (including the Go test-suite cases) against the datasets in
/// `tests/fixtures/datasets.json`. A handful of known, intentional deviations
/// are excluded from the fixture (see the crate docs).
#[test]
fn differential_against_go_implementation() {
    let datasets: Json = serde_json::from_str(include_str!("../tests/fixtures/datasets.json")).unwrap();
    let names: Vec<&str> = datasets.as_object().unwrap().keys().map(String::as_str).collect();
    let mut hard = Vec::new();
    let mut total = 0;
    for line in include_str!("../tests/fixtures/go_oracle.jsonl").lines() {
        let rec: Json = serde_json::from_str(line).unwrap();
        let expr = rec["e"].as_str().unwrap();
        let ds = rec["d"].as_str().unwrap();
        let go_err = rec.get("err").is_some();
        let go_t = normalize_go_type(rec["t"].as_str().unwrap()).to_string();
        let go_v = rec["v"].as_str().unwrap().to_string();
        let targets: Vec<&str> = if ds == "*" { names.clone() } else { vec![ds] };
        for name in targets {
            total += 1;
            let data = &datasets[name];
            let compiled = Expression::compile(expr);
            let (t, v, err) = match &compiled {
                Ok(e) => go_repr(e, data),
                Err(_) => ("bool".into(), "false".into(), true),
            };
            let ctx = format!("{expr} [{name}]");
            if go_err != err {
                hard.push(format!("{ctx}: go err={go_err} ({go_t} {go_v:?}); rs err={err} ({t} {v:?})"));
                continue;
            }
            // Nested container formatting of CEL-created values differs
            // cosmetically from Go's (e.g. "[1, 2]" vs "[1 2]"); only compare
            // the kind for lists/maps.
            let containers = (go_t == "list" || go_t == "map") && go_t == t;
            if !go_err && !containers && (go_t != t || go_v != v) {
                hard.push(format!("{ctx}: go {go_t} {go_v:?}; rs {t} {v:?}"));
                continue;
            }
            if let (Ok(e), Some(go_ok)) = (&compiled, rec.get("r").and_then(Json::as_bool))
                && e.validate_restrictive().is_ok() != go_ok
            {
                hard.push(format!("{ctx}: restrictive go={go_ok} rs={:?}", e.validate_restrictive()));
            }
        }
    }
    assert!(total > 6000, "fixture too small: {total}");
    assert!(
        hard.is_empty(),
        "{} of {total} cases differ from Go:\n{}",
        hard.len(),
        hard.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Typical Inngest expressions
// ---------------------------------------------------------------------------

#[test]
fn typical_trigger_and_wait_expressions() {
    let event = ev(json!({"userId": "u1", "amount": 150, "plan": "pro", "tags": ["a", "b"]}));
    let asyn = ev(json!({"userId": "u1", "billing_plan": "enterprise"}));
    let vars = Vars {
        event: Some(&event),
        async_: Some(&asyn),
    };
    let t = |s: &str| Expression::compile(s).unwrap().eval_bool(vars).unwrap();
    assert!(t("event.data.userId == async.data.userId"));
    assert!(t("event.data.amount > 100 && event.data.plan == 'pro'"));
    assert!(!t("event.data.amount > 200 && event.data.plan == 'pro'"));
    assert!(t("async.data.billing_plan == 'enterprise'"));
    assert!(t("event.name == 'app/test' && 'a' in event.data.tags"));
    assert!(!t("event.data.missing == async.data.userId"));
    assert!(t(r#"event.data.plan in ["pro", "enterprise"]"#));
    assert!(t("event.data.tags.exists(t, t == 'b')"));
    assert!(t("has(event.data.plan) && !has(event.data.nope)"));
    assert!(t("event.data.amount >= 150.0 && event.data.amount == 150 && event.data.amount == 150u"));
    // Missing async (e.g. evaluating a pause expression without an event).
    let only_event = Vars {
        event: Some(&event),
        async_: None,
    };
    let e = Expression::compile("event.data.userId == async.data.userId").unwrap();
    assert!(!e.eval_bool(only_event).unwrap());
    assert!(!e.eval_bool(Vars::default()).unwrap());
}

#[test]
fn string_functions() {
    let d = json!({"email": "Alice@Example.com", "title": "nope", "enc": "eyJuZXN0ZWQiOnsiZGF0YSI6InllYSJ9fQ=="});
    assert!(bool_data("event.data.email.endsWith('Example.com')", d.clone()));
    assert!(bool_data("event.data.email.startsWith('Alice')", d.clone()));
    assert!(bool_data("event.data.email.contains('@')", d.clone()));
    assert!(bool_data("lowercase(event.data.email) == 'alice@example.com'", d.clone()));
    assert!(bool_data("uppercase(event.data.title) == 'NOPE'", d.clone()));
    assert!(bool_data("event.data.title.matches('^[a-z]+$')", d.clone()));
    assert!(bool_data("matches(event.data.title, 'o')", d.clone()));
    assert!(bool_data("size(event.data.title) == 4 && event.data.title.size() == 4", d.clone()));
    assert!(bool_data("size('héllo') == 5", d.clone()));
    assert_eq!(eval_data("b64decode('aGkgdGhlcmUh')", d.clone()), json!("hi there!"));
    assert_eq!(eval_data("json_parse(b64decode(event.data.enc)).nested.data", d.clone()), json!("yea"));
    assert_eq!(eval_data("json_parse('[1]')", d.clone()), json!({}));
    assert_eq!(eval_data("json_parse('{\"a\": 1}').a", d.clone()), json!(1.0));
    // Non-string inputs to the custom string functions behave like Go's
    // unchecked type assertion: the empty string.
    assert_eq!(eval_data("lowercase(5)", d.clone()), json!(""));
    assert_eq!(eval_data("b64decode(5)", d.clone()), json!(""));
    // Invalid base64 decodes to what was decoded before the error.
    assert_eq!(eval_data("b64decode('aGk')", d.clone()), json!(""));
    assert_eq!(eval_data("b64decode('aGkgdGhlcmUh!!!!')", d), json!("hi there!"));
}

#[test]
fn in_operator_and_macros() {
    let d = json!({"tags": ["a", "b"], "nums": [1, 2, 3], "items": [{"n": "x", "q": 2}, {"n": "y", "q": 0}], "obj": {"k": 1}});
    assert!(bool_data("'a' in event.data.tags", d.clone()));
    assert!(!bool_data("'z' in event.data.tags", d.clone()));
    assert!(bool_data("2 in event.data.nums && 2.0 in event.data.nums && 2u in event.data.nums", d.clone()));
    assert!(bool_data("'k' in event.data.obj", d.clone()));
    assert!(bool_data("event.data.nums.all(n, n > 0)", d.clone()));
    assert!(!bool_data("event.data.nums.all(n, n > 1)", d.clone()));
    assert!(bool_data("event.data.nums.exists(n, n == 3)", d.clone()));
    assert!(bool_data("event.data.nums.exists_one(n, n > 2)", d.clone()));
    assert!(bool_data("event.data.nums.existsOne(n, n > 2)", d.clone()));
    assert!(!bool_data("event.data.nums.exists_one(n, n > 1)", d.clone()));
    assert_eq!(eval_data("event.data.nums.map(n, n * 2.0)", d.clone()), json!([2.0, 4.0, 6.0]));
    assert_eq!(eval_data("[1, 2, 3].map(n, n * 2)", d.clone()), json!([2, 4, 6]));
    assert_eq!(eval_data("[1, 2, 3].map(n, n > 1, n * 10)", d.clone()), json!([20, 30]));
    assert_eq!(eval_data("event.data.items.filter(i, i.q > 0).map(i, i.n)", d.clone()), json!(["x"]));
    assert_eq!(eval_data("[[1, 2], [3]].map(l, l.size())", d.clone()), json!([2, 1]));
    assert!(bool_data("[[1], [2, 3]].exists(l, l.exists(x, x == 3))", d.clone()));
    assert!(bool_data("{'a': 1, 'b': 2}.all(k, k.size() == 1)", d.clone()));
    // Macros over missing data are unknown, i.e. false at the top level.
    assert!(!bool_data("event.data.missing.exists(x, x == 'a')", d.clone()));
    assert!(!bool_data("event.data.missing.all(x, x == 'a')", d.clone()));
    // Macros over non-iterables error.
    assert!(matches!(eval_err("event.data.nums[0].exists(x, true)", d.clone()), ExprError::Eval(_)));
    // Shadowing and nested scopes.
    assert!(bool_data("[1, 2].exists(x, [3, 4].exists(x, x == 4))", d.clone()));
    assert!(bool_data("[1, 2].all(x, [x].all(y, y == x))", d));
}

// ---------------------------------------------------------------------------
// Unknown / null rules (ported from Go's unknown_test.go)
// ---------------------------------------------------------------------------

#[test]
fn unknown_rules() {
    let none = json!({});
    let cases: &[(&str, Json)] = &[
        ("event.data.missing == 'foo'", json!(false)),
        ("event.data.missing == null", json!(true)),
        ("event.data.missing == 0", json!(false)),
        ("event.data.a == event.data.b", json!(false)),
        ("event.data.missing != 'foo'", json!(true)),
        ("event.data.missing != null", json!(false)),
        ("event.data.missing != 0", json!(true)),
        ("event.data.missing != ''", json!(true)),
        ("event.data.missing < 5", json!(true)),
        ("event.data.missing <= 5", json!(true)),
        ("5 < event.data.missing", json!(false)),
        ("5 <= event.data.missing", json!(false)),
        ("event.data.missing > 5", json!(false)),
        ("event.data.missing >= 5", json!(false)),
        ("5 > event.data.missing", json!(true)),
        ("5 >= event.data.missing", json!(true)),
        ("event.data.a + event.data.b", json!(false)),
        ("size(event.data.missing)", json!(0)),
        ("size(event.data.missing) > 0", json!(false)),
        ("size(event.data.missing) == 0", json!(true)),
        ("event.data.missing.size()", json!(0)),
        ("event.data.missing in ['a', 'b', 'c']", json!(false)),
        ("event.data.some.unknown.contains('x')", json!(false)),
        ("lowercase(event.data.missing)", json!(false)),
        ("!event.data.missing", json!(false)),
        ("-event.data.missing", json!(false)),
        ("event.data.missing + 1", json!(1)),
        ("1 + event.data.missing", json!(1)),
        ("event.data.missing + 1 + 2", json!(3)),
    ];
    for (src, want) in cases {
        assert_eq!(&eval_data(src, none.clone()), want, "{src}");
    }
    let d = json!({"n": 2, "name": "hello"});
    assert!(bool_data("event.data.n > event.data.missing", d.clone()));
    assert_eq!(eval_data("event.data.name + event.data.missing", d.clone()), json!("hello"));
    assert_eq!(eval_data("event.data.missing + event.data.name", d.clone()), json!("hello"));
    // Whole-result unknown: null from eval(), false from eval_bool().
    assert_eq!(eval_data("event.data.nonexistent", d.clone()), Json::Null);
    assert!(!bool_data("event.data.nonexistent", d.clone()));
    // Deeply missing / traversing through null and scalars is unknown.
    assert!(bool_data("event.data.issue.field.another != 'another'", json!({"issue": null})));
    assert!(!bool_data("event.data.issue.field.another == 'another'", json!({"issue": null})));
    assert!(bool_data("event.data.name.first == null", d.clone()));
    // A referenced, missing descendant makes its (present) parent unknown too,
    // exactly like Inngest's attribute patterns.
    assert_eq!(
        eval_data("size(event.data.obj) + (event.data.obj.zz == 1 ? 1 : 0)", json!({"obj": {"a": 1}})),
        json!(0)
    );
    assert_eq!(eval_data("size(event.data.obj)", json!({"obj": {"a": 1}})), json!(1));
}

#[test]
fn errors_outside_root_paths() {
    let d = json!({"items": [{"name": "a"}], "obj": {"a": 1}});
    // Missing keys after an index / on computed values are errors (as in Go),
    // which comparisons treat like unknowns.
    assert!(matches!(eval_err("event.data.items[0].nope", d.clone()), ExprError::Eval(_)));
    assert!(bool_data("event.data.items[0].nope == null", d.clone()));
    assert!(matches!(eval_err("event.data.items[5]", d.clone()), ExprError::Eval(_)));
    assert!(bool_data("event.data.items[5] == null", d.clone()));
    assert!(!bool_data("event.data.items[5] == 'x'", d.clone()));
    assert!(matches!(eval_err("json_parse('{}').a", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("event['data'].missing", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("event.data.items['x']", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("1 / 0", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("5 % 0", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("9223372036854775807 + 1", d.clone()), ExprError::Eval(_)));
    // Errors become concrete values in calls.
    assert!(bool_data("(1/0) < 1", d.clone()));
    assert!(!bool_data("(1/0) > 1", d.clone()));
    // `+` with an unknown returns the other operand, even an error.
    assert!(matches!(eval_err("(1/0) + event.data.missing", d.clone()), ExprError::Eval(_)));
    assert_eq!(eval_data("event.data.missing + (1/0) == null", d.clone()), json!(true));
    // Undeclared `async`: no such attribute.
    assert!(matches!(eval_err("async", d.clone()), ExprError::Eval(_)));
    assert!(!bool_data("async.data.x", d.clone()));
    assert!(bool_data("async.data.x == null", d));
}

#[test]
fn null_vs_concrete() {
    let none = json!({});
    let cases: &[(&str, bool)] = &[
        ("null == 'foo'", false),
        ("'foo' == null", false),
        ("null != 'foo'", true),
        ("'foo' != null", true),
        ("null < 'foo'", true),
        ("'foo' < null", false),
        ("null < uint(5)", true),
        ("null < 5", true),
        ("5 < null", false),
        ("null > 0", false),
        ("5 > null", true),
        ("null <= 0", true),
        ("null >= 1", false),
        ("null < 0.5", true),
        ("null == null", true),
    ];
    for (src, want) in cases {
        assert_eq!(bool_data(src, none.clone()), *want, "{src}");
    }
    let d = json!({"float": 3.25, "n": 8, "nul": null});
    assert!(bool_data("event.data.float > null", d.clone()));
    assert!(bool_data("event.data.n > null", d.clone()));
    assert!(!bool_data("event.data.float <= null", d.clone()));
    assert!(bool_data("event.data.nul == null", d.clone()));
    assert!(bool_data("event.data.nul == event.data.missing", d.clone()));
    assert!(!bool_data("size(event.data.nul) > 0", d.clone()));
    assert!(!bool_data("event.data.nul > 0", d.clone()));
    assert!(bool_data("event.data.nul < 1", d.clone()));
    // null compared with a non-zeroable type is an error.
    assert!(matches!(eval_err("event.data.nul < [1]", d), ExprError::Eval(_)));
}

#[test]
fn negated_presence() {
    let cases: &[(&str, Json, bool)] = &[
        ("!has(event.data.missing)", json!({}), true),
        ("has(event.data.user.active)", json!({}), false),
        ("!has(event.data.user.active)", json!({}), true),
        ("!has(event.data.user.active)", json!({"user": []}), false),
        ("!has(event.data.items[0].name)", json!({"items": []}), false),
        ("!event.data.missing", json!({}), false),
        ("!event.data.value.matches('x')", json!({"value": 1}), false),
        ("!has(event.data.user) || event.data.user.active == true", json!({}), true),
        ("!has(event.data.user) || event.data.user.active == true", json!({"user": {}}), false),
        ("!has(event.data.user) && event.data.user.active == null", json!({}), true),
        ("!has(event.data.user) && event.data.user.active == null", json!({"user": {}}), false),
        ("has(event.data.user.active)", json!({"user": {"active": false}}), true),
        ("has(event.data.user.active)", json!({"user": {"active": null}}), true),
        ("has(event.data.nul.x)", json!({"nul": null}), false),
        ("has(event.data.s.x)", json!({"s": "str"}), false),
        ("has({'a': 1}.a) && !has({'a': 1}.b)", json!({}), true),
        ("has(async.data)", json!({}), false),
    ];
    for (src, data, want) in cases {
        let e = ev(data.clone());
        let got = Expression::compile(src)
            .unwrap()
            .eval_bool(Vars {
                event: Some(&e),
                async_: None,
            })
            .unwrap_or_else(|err| panic!("{src}: {err}"));
        assert_eq!(got, *want, "{src} with {data}");
    }
    // has() over a list parent is an error (as in Go).
    assert!(matches!(eval_err("has(event.data.user.active)", json!({"user": []})), ExprError::Eval(_)));
    // a, b, c must all be false
    let src = "!((has(event.data.a) && event.data.a) || (has(event.data.b) && event.data.b) || (has(event.data.c) && event.data.c))";
    assert!(bool_data(src, json!({"a": false, "b": false, "c": false})));
    assert!(!bool_data(src, json!({"a": true, "b": false, "c": false})));
    assert!(bool_data(src, json!({})));
}

// ---------------------------------------------------------------------------
// Truthiness
// ---------------------------------------------------------------------------

#[test]
fn truthy_logical_operators() {
    let cases: &[(&str, Json, Json)] = &[
        ("event.data.a || event.data.b", json!({"a": "first", "b": "second"}), json!("first")),
        ("event.data.a || event.data.b", json!({"a": "", "b": "second"}), json!("second")),
        ("event.data.missing || event.data.b", json!({"b": "found"}), json!("found")),
        ("event.data.a || event.data.missing", json!({"a": "found"}), json!("found")),
        ("event.data.a || event.data.b", json!({}), Json::Null),
        ("event.data.zero || event.data.name", json!({"zero": 0, "name": "hi"}), json!("hi")),
        ("null || event.data.b", json!({"b": "fallback"}), json!("fallback")),
        ("event.data.f || false", json!({"f": 3.25}), json!(3.25)),
        ("event.data.f || event.data.b", json!({"f": 0.0, "b": "yes"}), json!("yes")),
        ("false || event.data.list", json!({"list": ["a"]}), json!(["a"])),
        ("false || event.data.list", json!({"list": []}), json!([])),
        ("uint(5) || false", json!({}), json!(5)),
        ("uint(0) || event.data.b", json!({"b": "yes"}), json!("yes")),
        ("event.data.a || event.data.b", json!({"a": false, "b": true}), json!(true)),
        ("event.data.a || event.data.b", json!({"a": false, "b": false}), json!(false)),
        ("event.data.a && event.data.b", json!({"a": "first", "b": "second"}), json!("second")),
        ("event.data.a && event.data.b", json!({"a": "", "b": "second"}), json!("")),
        ("event.data.a && event.data.b", json!({"a": "first", "b": ""}), json!("")),
        ("event.data.missing && event.data.b", json!({"b": "x"}), Json::Null),
        ("event.data.a && event.data.b", json!({"a": true, "b": false}), json!(false)),
        ("event.data.a && event.data.b", json!({"a": true, "b": true}), json!(true)),
        ("0 && true", json!({}), json!(0)),
        ("true && 0", json!({}), json!(0)),
        ("false || ''", json!({}), json!("")),
        ("(1 / 0 == 1) || true", json!({}), json!(true)),
        ("false && (1 / 0 == 1)", json!({}), json!(false)),
        ("(1 / 0 == 1) && false", json!({}), json!(false)),
    ];
    for (src, data, want) in cases {
        assert_eq!(&eval_data(src, data.clone()), want, "{src} with {data}");
    }
    // An error as the deciding operand propagates.
    assert!(matches!(eval_err("false || (1 / 0)", json!({})), ExprError::Eval(_)));
    // N-ary chains.
    let src = "event.data.user.name == 'alice' && event.data.user.role == 'admin' && (event.data.f.a || event.data.f.b || event.data.f.c)";
    let user = json!({"name": "alice", "role": "admin"});
    assert_eq!(eval_data(src, json!({"user": user, "f": {"c": "on"}})), json!("on"));
    assert_eq!(eval_data(src, json!({"user": user, "f": {"a": "on"}})), json!("on"));
    assert_eq!(eval_data(src, json!({"user": user, "f": {}})), Json::Null);
    assert_eq!(eval_data(src, json!({"user": {"name": "bob", "role": "admin"}, "f": {"a": "on"}})), json!(false));
}

#[test]
fn truthy_ternary() {
    let src = "event.data.value ? 'truthy' : 'falsy'";
    let cases: &[(Json, &str)] = &[
        (json!({}), "falsy"),
        (json!({"value": null}), "falsy"),
        (json!({"value": false}), "falsy"),
        (json!({"value": ""}), "falsy"),
        (json!({"value": 0}), "falsy"),
        (json!({"value": 0.0}), "falsy"),
        (json!({"value": true}), "truthy"),
        (json!({"value": "candidate-123"}), "truthy"),
        (json!({"value": []}), "truthy"),
        (json!({"value": {}}), "truthy"),
        (json!({"value": 1.5}), "truthy"),
    ];
    for (data, want) in cases {
        assert_eq!(eval_data(src, data.clone()), json!(want), "{data}");
    }
    assert_eq!(
        eval_data(
            "event.data.primaryCandidateId ? event.data.primaryCandidateId : event.data.candidateId",
            json!({"candidateId": "candidate-123"})
        ),
        json!("candidate-123")
    );
    assert_eq!(eval_data("(1/0) ? 1 : 2", json!({})), json!(2));
}

// ---------------------------------------------------------------------------
// Numbers, concatenation, conversions
// ---------------------------------------------------------------------------

#[test]
fn numeric_semantics() {
    let d = json!({"n": 5, "f": 1.5, "big": 1234567});
    assert!(bool_data("1 == 1.0 && 1u == 1.0 && 1 == 1u", d.clone()));
    assert!(bool_data("event.data.n == 5 && event.data.n == 5u && event.data.n == 5.0", d.clone()));
    assert!(bool_data("event.data.n > 3.141 && event.data.f <= 2", d.clone()));
    assert!(bool_data("-1 < 1u && 2u > 1.5", d.clone()));
    assert!(bool_data("[1, 2] == [1.0, 2u] && {'a': 1} == {'a': 1.0}", d.clone()));
    assert!(bool_data("{1: 'a'}[1.0] == 'a' && [1, 2][1u] == 2", d.clone()));
    // JSON numbers are doubles; `+` with an int literal concatenates (Go quirk).
    assert_eq!(eval_data("event.data.n + 1", d.clone()), json!("51"));
    assert_eq!(eval_data("event.data.n + 1.0", d.clone()), json!(6.0));
    assert_eq!(eval_data("int(event.data.n) + 1", d.clone()), json!(6));
    // Other cross-type arithmetic is an error, as in Go.
    assert!(matches!(eval_err("event.data.n - 1", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("event.data.n * 2", d.clone()), ExprError::Eval(_)));
    assert_eq!(eval_data("event.data.n * 2.0", d.clone()), json!(10.0));
    assert_eq!(eval_data("7 / 2", d.clone()), json!(3));
    assert_eq!(eval_data("-7 % 2", d.clone()), json!(-1));
    assert_eq!(eval_data("7u / 2u", d.clone()), json!(3));
    assert_eq!(eval_data("10.0 / 4.0", d.clone()), json!(2.5));
    assert_eq!(eval_data("1.0 / 0.0", d.clone()), Json::Null); // +Inf has no JSON form
    assert!(matches!(eval_err("-9223372036854775808 / -1", d.clone()), ExprError::Eval(_)));
    assert!(matches!(eval_err("1u - 2u", d.clone()), ExprError::Eval(_)));
    assert_eq!(eval_data("-9223372036854775808", d.clone()), json!(i64::MIN));
    assert_eq!(eval_data("18446744073709551615u", d.clone()), json!(u64::MAX));
    assert_eq!(eval_data("0x1F + 1", d.clone()), json!(32));
    assert_eq!(eval_data("--1", d.clone()), json!(1));
    assert_eq!(eval_data("---1", d.clone()), json!(-1));
}

#[test]
fn concatenation_of_mixed_types() {
    let d = json!({
        "name": "hi", "number": 9.1, "bool": true, "list": [3, "ok?", true], "obj": {"name": "inngest"}
    });
    assert_eq!(
        eval_data(
            "event.data.name + '-' + event.data.number + '-' + event.data.bool + '-' + event.data.list + '-' + event.data.obj + '-' + event.data.missing",
            d.clone()
        ),
        json!("hi-9.1-true-[3 ok? true]-map[name:inngest]-")
    );
    assert_eq!(eval_data("'x' + 1", d.clone()), json!("x1"));
    assert_eq!(eval_data("1 + 'x'", d.clone()), json!("1x"));
    assert_eq!(eval_data("1 + 1u", d.clone()), json!("11"));
    assert_eq!(eval_data("1.5 + 1", d.clone()), json!("1.51"));
    assert_eq!(eval_data("'' + 1234567.0", d.clone()), json!("1.234567e+06"));
    assert_eq!(eval_data("'' + 0.00001", d.clone()), json!("1e-05"));
    assert_eq!(eval_data("'' + duration('90m')", d.clone()), json!("1h30m0s"));
    assert_eq!(
        eval_data("'' + timestamp('2020-01-01T00:00:00Z')", d.clone()),
        json!("2020-01-01 00:00:00 +0000 UTC")
    );
    assert_eq!(eval_data("'k-' + event.data.nul", json!({"nul": null})), json!("k-NULL_VALUE"));
    assert_eq!(eval_data("'x' + event.data.list[9]", d), json!("xindex out of bounds: 9"));
}

#[test]
fn conversions() {
    let d = json!({"s": "42", "f": 1.9});
    assert_eq!(eval_data("int(event.data.s)", d.clone()), json!(42));
    assert_eq!(eval_data("int(event.data.f)", d.clone()), json!(1));
    assert_eq!(eval_data("int(-1.9)", d.clone()), json!(-1));
    assert_eq!(eval_data("uint(event.data.s)", d.clone()), json!(42));
    assert_eq!(eval_data("double(event.data.s)", d.clone()), json!(42.0));
    assert_eq!(eval_data("string(event.data.f)", d.clone()), json!("1.9"));
    assert_eq!(eval_data("string(5)", d.clone()), json!("5"));
    assert_eq!(eval_data("string(true)", d.clone()), json!("true"));
    assert_eq!(eval_data("string(duration('90m'))", d.clone()), json!("5400s"));
    assert_eq!(eval_data("string(timestamp('2020-01-01T00:00:00.5Z'))", d.clone()), json!("2020-01-01T00:00:00.5Z"));
    assert_eq!(
        eval_data("string(timestamp('2020-01-01T10:00:00+05:00'))", d.clone()),
        json!("2020-01-01T10:00:00+05:00")
    );
    assert_eq!(eval_data("bool('true') && bool('t') && !bool('0')", d.clone()), json!(true));
    assert_eq!(eval_data("bytes('hi')", d.clone()), json!("aGk="));
    assert_eq!(eval_data("string(b'hi')", d.clone()), json!("hi"));
    assert_eq!(eval_data("dyn(5) == 5", d.clone()), json!(true));
    assert_eq!(eval_data("type(event.data.f)", d.clone()), json!("double"));
    assert_eq!(eval_data("type(1) == int && type('a') == string && type(null) == null_type", d.clone()), json!(true));
    assert_eq!(eval_data("type([]) == list && type({}) == map && type(type(1)) == type", d.clone()), json!(true));
    for bad in ["int('abc')", "int(1e20)", "uint(-1)", "double('x')", "bool('yes')", "timestamp('bad')", "duration('1d')"] {
        assert!(matches!(eval_err(bad, d.clone()), ExprError::Eval(_)), "{bad}");
    }
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

#[test]
fn timestamps_and_durations() {
    let d = json!({"from": "2020-01-01T12:00:00", "date": "2024-05-01T10:00:00Z", "ts": 1774512121538.0});
    assert!(bool_data(
        "date(event.data.from) + duration('6h35m10s') == date('2020-01-01T18:35:10')",
        d.clone()
    ));
    assert!(bool_data("date(event.data.date) > date('2024-01-01')", d.clone()));
    assert!(bool_data("date(event.data.date) < now()", d.clone()));
    assert!(bool_data("now_plus('1h') > now() && now_minus('1d') < now()", d.clone()));
    assert!(bool_data("now_plus('2d') > now_plus('47h') && now_plus('1w') > now_plus('6d')", d.clone()));
    assert!(bool_data("now_plus('1.5h') == now_plus('90m') && now_plus('1h30m') == now_plus('90m')", d.clone()));
    assert!(bool_data("date(1774512121) == timestamp(1774512121)", d.clone()));
    assert!(bool_data("date(1774512121538).getMilliseconds() == 538", d.clone()));
    // Lenient: Go rejects doubles in date(), but JSON numbers are doubles.
    assert!(bool_data("date(event.ts) == date(1774512121538)", d.clone()));
    assert_eq!(eval_data("date(event.data.date)", d.clone()), json!("2024-05-01T10:00:00Z"));
    assert_eq!(eval_data("date('2020-01-01T00:00:00+05:00')", d.clone()), json!("2020-01-01T00:00:00+05:00"));
    assert_eq!(eval_data("duration('1h')", d.clone()), json!("1h0m0s"));
    assert_eq!(eval_data("timestamp('2020-01-01T00:00:00Z') - timestamp('2019-12-31T00:00:00Z')", d.clone()), json!("24h0m0s"));
    assert!(bool_data("duration('1h') + duration('1m') == duration('61m')", d.clone()));
    assert!(bool_data("duration('1h') > duration('30m')", d.clone()));
    assert!(bool_data("timestamp('2020-01-01T00:00:00Z') == timestamp('2020-01-01T01:00:00+01:00')", d.clone()));
    let getters: &[(&str, i64)] = &[
        ("timestamp('2020-03-05T10:20:30.456Z').getFullYear()", 2020),
        ("timestamp('2020-03-05T10:20:30.456Z').getMonth()", 2),
        ("timestamp('2020-03-01T00:00:00Z').getDayOfYear()", 60),
        ("timestamp('2020-03-05T10:20:30.456Z').getDayOfMonth()", 4),
        ("timestamp('2020-03-05T10:20:30.456Z').getDate()", 5),
        ("timestamp('2020-03-05T10:20:30.456Z').getDayOfWeek()", 4),
        ("timestamp('2020-03-05T10:20:30.456Z').getHours()", 10),
        ("timestamp('2020-03-05T10:20:30.456Z').getMinutes()", 20),
        ("timestamp('2020-03-05T10:20:30.456Z').getSeconds()", 30),
        ("timestamp('2020-03-05T10:20:30.456Z').getMilliseconds()", 456),
        ("timestamp('2020-03-05T10:20:30.456Z').getHours('+05:30')", 15),
        ("timestamp('2020-03-05T10:20:30.456Z').getMinutes('-05:30')", 50),
        ("timestamp('2020-03-05T10:20:30.456Z').getHours('Asia/Tokyo')", 19),
        ("timestamp('2020-01-01T10:00:00+05:00').getHours()", 5),
        ("duration('90m').getHours()", 1),
        ("duration('90m').getMinutes()", 90),
        ("duration('90m').getSeconds()", 5400),
        ("duration('1.5s').getMilliseconds()", 1500),
    ];
    for (src, want) in getters {
        assert_eq!(eval_data(src, d.clone()), json!(want), "{src}");
    }
    assert!(matches!(eval_err("timestamp('2020-01-01T00:00:00Z').getHours('Nope/Zone')", d.clone()), ExprError::Eval(_)));
    // Failed date()/now_plus() produce Go nils: falsy in logic, fatal elsewhere.
    assert!(bool_data("date('bad') || true", d.clone()));
    assert_eq!(eval_data("date('bad') ? 1 : 2", d.clone()), json!(2));
    assert!(!bool_data("date('bad') == event.data.missing", d.clone()));
    for bad in ["date('bad')", "now_plus('bad') > now()", "now_plus(5) > now()", "date('bad') + duration('1h')", "size(date('bad'))"] {
        let err = eval_err(bad, d.clone());
        assert!(err.to_string().contains("did not return true or false"), "{bad}: {err}");
    }
    // date() of a missing value is `false` (unknown call result), so ordering it
    // against a timestamp errors, as in Go.
    assert_eq!(eval_data("date(event.data.missing)", d.clone()), json!(false));
    assert!(matches!(eval_err("date(event.data.missing) < now()", d), ExprError::Eval(_)));
}

// ---------------------------------------------------------------------------
// Strings & literals
// ---------------------------------------------------------------------------

#[test]
fn string_literals_are_lifted_raw() {
    // Inngest lifts plain quoted literals verbatim: escapes are not processed.
    assert_eq!(eval_data(r#""a\nb""#, json!({})), json!("a\\nb"));
    assert_eq!(eval_data(r#"size("a\nb")"#, json!({})), json!(4));
    assert_eq!(eval_data(r#"'it\'s'"#, json!({})), json!("it\\'s"));
    assert_eq!(eval_data(r#""a\"b" == 'a"b'"#, json!({})), json!(false));
    assert!(bool_data(r"event.data.title.matches('\w')", json!({"title": "nope"})));
    assert!(bool_data(r"'a1'.matches('\d')", json!({})));
    // ...unless the expression mentions `vars.`, which disables lifting.
    assert_eq!(eval_data(r#""vars." + "\n""#, json!({})), json!("vars.\n"));
    // Raw, triple-quoted and bytes literals use CEL semantics.
    assert_eq!(eval_data(r#"r"a\n""#, json!({})), json!("a\\n"));
    assert_eq!(eval_data("'''multi\nline'''", json!({})), json!("multi\nline"));
    assert_eq!(eval_data(r#""""x"y\n""""#, json!({})), json!("x\"y\n"));
    assert_eq!(eval_data(r#"b"\x41\101" == bytes("AA")"#, json!({})), json!(true));
    assert_eq!(eval_data("'héllo' + \"wörld\"", json!({})), json!("héllowörld"));
    assert_eq!(eval_data("event.name // a comment\n + '!'", json!({})), json!("app/test!"));
}

#[test]
fn duplicate_map_keys_last_wins() {
    assert_eq!(eval_data("{'a': 1, 'a': 2}", json!({})), json!({"a": 2}));
    assert_eq!(eval_data("{1: 'x', 2: 'y'}", json!({})), json!({"1": "x", "2": "y"}));
}

// ---------------------------------------------------------------------------
// Public API: eval / eval_bool / eval_key / eval_int
// ---------------------------------------------------------------------------

#[test]
fn eval_json_conversion() {
    let d = json!({"n": 5, "list": [1, "a", null], "obj": {"a": [1]}});
    assert_eq!(eval_data("event.data.n", d.clone()), json!(5.0));
    assert_eq!(eval_data("5 + 4", d.clone()), json!(9));
    assert_eq!(eval_data("5u", d.clone()), json!(5u64));
    assert_eq!(eval_data("event.data.list", d.clone()), json!([1, "a", null]));
    assert_eq!(eval_data("event.data.obj", d.clone()), json!({"a": [1]}));
    assert_eq!(eval_data("[1, 'a', null, [true]]", d.clone()), json!([1, "a", null, [true]]));
    assert_eq!(eval_data("{'k': {'n': 1.5}}", d.clone()), json!({"k": {"n": 1.5}}));
    assert_eq!(eval_data("event.data.nul", d.clone()), Json::Null);
    assert_eq!(eval_data("b'hi'", d.clone()), json!("aGk="));
    assert_eq!(eval_data("type(1)", d), json!("int"));
}

#[test]
fn eval_bool_contexts() {
    let e = ev(json!({"s": "x", "n": 1}));
    let vars = Vars {
        event: Some(&e),
        async_: None,
    };
    let b = |s: &str| Expression::compile(s).unwrap().eval_bool(vars);
    assert!(b("event.data.s == 'x'").unwrap());
    assert!(!b("event.data.missing").unwrap());
    assert!(matches!(b("event.data.s"), Err(ExprError::Type(_))));
    assert!(matches!(b("event.data.n"), Err(ExprError::Type(_))));
    assert!(matches!(b("event.data.s || false"), Err(ExprError::Type(_))));
    assert!(matches!(b("1 / 0 == 1 || 1 / 0"), Err(ExprError::Eval(_))));
    assert!(matches!(b("event.data.s.x.y + 1 < 'a' + 1"), Ok(_) | Err(_)));
}

#[test]
fn key_formatting() {
    let d = json!({
        "ingressId": "IN_1", "userId": "u-1", "n": 1, "f": 1.5, "big": 1234567, "t": true,
        "nul": null, "list": [1, "a"], "obj": {"b": 1, "a": "x"}
    });
    assert_eq!(key("event.data.ingressId", d.clone()), "IN_1");
    assert_eq!(key("event.data.ingressId + '-' + event.data.userId", d.clone()), "IN_1-u-1");
    assert_eq!(key("event.data.ingressId || event.data.userId", d.clone()), "IN_1");
    assert_eq!(key("event.data.missing || event.data.userId", d.clone()), "u-1");
    assert_eq!(key("event.data.ingressId + event.data.missing", d.clone()), "IN_1");
    assert_eq!(key("event.data.n", d.clone()), "1");
    assert_eq!(key("event.data.f", d.clone()), "1.5");
    assert_eq!(key("event.data.big", d.clone()), "1.234567e+06");
    assert_eq!(key("event.data.t", d.clone()), "true");
    assert_eq!(key("event.data.nul", d.clone()), "<nil>");
    assert_eq!(key("event.data.missing", d.clone()), "<nil>");
    assert_eq!(key("event.data.list", d.clone()), "[1 a]");
    assert_eq!(key("event.data.obj", d.clone()), "map[a:x b:1]");
    assert_eq!(key("5 + 4", d.clone()), "9");
    assert_eq!(key("event.id", d.clone()), "01H");
    assert_eq!(key("event.name", d.clone()), "app/test");
    let e = ev(d.clone());
    let vars = Vars {
        event: Some(&e),
        async_: None,
    };
    let opt = |s: &str| Expression::compile(s).unwrap().eval_key_opt(vars).unwrap();
    assert_eq!(opt("event.data.userId"), Some("u-1".to_string()));
    assert_eq!(opt("false"), None);
    assert_eq!(opt("event.data.n > 5"), None);
    assert_eq!(opt("event.data.missing"), None);
    assert_eq!(opt("true"), Some("true".to_string()));
    assert_eq!(opt("event.data.nul"), Some("<nil>".to_string()));
    assert!(Expression::compile("1 / 0").unwrap().eval_key(vars).is_err());
}

#[test]
fn eval_int_for_priority() {
    let e = ev(json!({"p": 10, "f": 1.5, "s": "x"}));
    let vars = Vars {
        event: Some(&e),
        async_: None,
    };
    let i = |s: &str| Expression::compile(s).unwrap().eval_int(vars);
    assert_eq!(i("event.data.p").unwrap(), 10);
    assert_eq!(i("int(event.data.p) * 2").unwrap(), 20);
    assert_eq!(i("event.data.p > 5 ? 100 : -100").unwrap(), 100);
    assert_eq!(i("5u").unwrap(), 5);
    assert_eq!(i("-600").unwrap(), -600);
    assert!(matches!(i("event.data.f"), Err(ExprError::Type(_))));
    assert!(matches!(i("event.data.s"), Err(ExprError::Type(_))));
    assert!(matches!(i("event.data.missing"), Err(ExprError::Type(_))));
    assert!(matches!(i("1.0 / 0.0"), Err(ExprError::Type(_))));
    assert!(matches!(i("1 / 0"), Err(ExprError::Eval(_))));
}

#[test]
fn references_event() {
    let r = |s: &str| Expression::compile(s).unwrap().references_event();
    assert!(r("event.data.x == 1"));
    assert!(r("async.data.x == event.data.x"));
    assert!(r("[1].exists(x, x == event.data.n)"));
    assert!(r("has(event.data.x)"));
    assert!(r("{'a': 1}[event.data.k]"));
    assert!(!r("async.data.x == 'a'"));
    assert!(!r("[1].exists(event, event == 1)"));
    assert!(!r("1 + 2"));
}

#[test]
fn compile_errors() {
    for bad in [
        "",
        "1 +",
        "event.data.x ==",
        "(1",
        "[1, 2",
        "{'a': }",
        "foo == 1",
        "events.data.x == 1",
        "user.email.endsWith('x')",
        "event.data.x.unknownFn()",
        "unknownFn(1)",
        "size()",
        "size(1, 2)",
        "lowercase('a', 'b')",
        "now(1)",
        "has(event)",
        "[1].exists(1, true)",
        "[1].exists(x)",
        "exists(x, true)",
        "event.data.x == 'unterminated",
        "1 = 1",
        "a.?b",
        "Foo{a: 1}",
        "event.`data`",
        "9223372036854775808",
        "18446744073709551616u",
        "event.data.x in",
        "if == 1",
        "x.in",
        "1 ? 2",
        "1.5u",
    ] {
        assert!(
            matches!(Expression::compile(bad), Err(ExprError::Compile(_))),
            "{bad:?} should not compile"
        );
    }
    for good in [
        "vars.a == 1",
        "event",
        "async",
        ".event.data",
        "event.data['a-b'] == 1",
        "[1, 2,]",
        "{'a': 1,}",
        "true ? 1 : false ? 2 : 3",
        "event.data.x.size()",
        "-event.data.x",
        "!!!event.data.x",
        "  event.data.x == 1  // trailing comment",
        "event.data.list[0][1]['a'].b",
        "int(async.status) >= 400 &&\n event.error == true",
    ] {
        assert!(Expression::compile(good).is_ok(), "{good:?} should compile: {:?}", Expression::compile(good));
    }
}

// ---------------------------------------------------------------------------
// Restrictive validation (ported from Go's validation_test.go)
// ---------------------------------------------------------------------------

#[test]
fn restrictive_validation() {
    let cases: &[(&str, bool)] = &[
        ("event.data.foo == 1", true),
        ("event.name.endsWith(\"foo\")", false),
        ("has(event.data.isPreview)", false),
        (
            r#""lol" == event.data || event.name == "foo" || (async.source == 'test' || async.source == 'google') && date(async.next_visit) > now_plus("24h") && async.email == 'test@example.com'"#,
            false,
        ),
        ("event.data.action == 'opened'", true),
        ("event.data.name", true),
        ("5 + 4", false),
        ("uppercase(event.data.name) + ' is my name'", false),
        (r#"b64decode("aGkgdGhlcmUh") + " - b64 data""#, false),
        ("json_parse(b64decode(event.data.encoded)).nested.data", false),
        ("null > 0", true),
        ("size(event.data.nope) > 0", false),
        ("event.data.float <= null && event.data.int < null", true),
        ("event.data.int > 3.141 && event.data.float <= 2", true),
        ("event.first.foo == 'test' && event.second.bar != true && event.second.baz != true", true),
        ("async.first.foo != 'test' && (async.first.bar == true || async.second.baz == true)", true),
        ("event.data.foo != '' && 'item' in event.data.issue.fields.tags", true),
        ("event['data'].foo != '' && event['data'].bar != true", true),
        ("event.data.test == true && event.data.issue.field.another != 'another'", true),
        ("event.nonexistent.exists(x, x == 'a')", false),
        ("event.data.title.matches('^[a-z\\\\d]{0,10}$')", false),
        ("event.data.some.unknown.contains(event.data.title)", false),
        ("int(async.status) == 200", true),
        ("uint(async.status) == 200u && double(async.n) > 1.5", true),
        ("event.data.tags.exists(x, x == 'd') == false", false),
        ("event.data.issue in ['Bug', 'Issue', 'Epic'] == true", true),
        ("[1, 2, 3].all(x, x > 0) && event.status == '200'", false),
        ("date(event.from) + duration('6h35m10s') == date('2020-01-01T18:35:10')", false),
        ("event.data.x > -1", true),
        ("-event.data.x > 1", false),
        ("!(event.data.x == 1) ? event.data.y : event.data.z", true),
        ("event.data.items[event.data.idx] == 'a'", true),
        ("event.data.items[size(event.data.idx)] == 'a'", false),
        ("event.data.x in [1 + 2]", true), // Go doesn't inspect list literals
        ("event.data.x.size() > 1", false),
        ("string(event.data.x) == '1'", false),
        ("event.data.x == async.data.x", true),
    ];
    for (src, valid) in cases {
        let e = Expression::compile(src).unwrap_or_else(|err| panic!("{src}: {err}"));
        let res = e.validate_restrictive();
        assert_eq!(res.is_ok(), *valid, "{src}: {res:?}");
        if let Err(err) = res {
            assert!(matches!(err, ExprError::Policy(_)));
            assert!(err.to_string().starts_with("invalid expression: "));
        }
    }
    let err = Expression::compile("has(event.a) && event.b.startsWith('x') && [1].all(x, true)")
        .unwrap()
        .validate_restrictive()
        .unwrap_err()
        .to_string();
    assert!(err.contains("uses 'has'") && err.contains("uses 'startsWith'") && err.contains("uses 'all'"), "{err}");
}

// ---------------------------------------------------------------------------
// Cache, threads, robustness
// ---------------------------------------------------------------------------

#[test]
fn expression_is_send_sync_clone() {
    fn assert_traits<T: Send + Sync + Clone + std::fmt::Debug>() {}
    assert_traits::<Expression>();
    fn assert_cache<T: Send + Sync>() {}
    assert_cache::<ExprCache>();
    let e = Expression::compile("event.data.x == 1").unwrap();
    assert_eq!(e.source(), "event.data.x == 1");
    assert!(format!("{e:?}").contains("event.data.x == 1"));
}

#[test]
fn cache_compiles_once_and_evicts() {
    let cache = ExprCache::new(16);
    let a = cache.get("event.data.x == 1").unwrap();
    let b = cache.get("event.data.x == 1").unwrap();
    assert!(Arc::ptr_eq(&a.inner, &b.inner));
    assert!(cache.get("event.data.x ==").is_err());
    for i in 0..100 {
        cache.get(&format!("event.data.x == {i}")).unwrap();
        assert!(cache.len() <= 16);
    }
    let unbounded = ExprCache::new(0);
    assert!(unbounded.get("1 == 1").is_ok());
    assert_eq!(unbounded.len(), 0);
    let _ = ExprCache::default();
}

#[test]
fn concurrent_evaluation() {
    let cache = Arc::new(ExprCache::default());
    let src = "event.data.missing < 5 && event.data.name != '' && (event.data.tag || event.data.fallback)";
    let handles: Vec<_> = (0..16)
        .map(|n| {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let e = cache.get(src).unwrap();
                for i in 0..200 {
                    let event = ev(json!({"name": format!("user-{n}-{i}"), "fallback": format!("tag-{i}")}));
                    let got = e
                        .eval(Vars {
                            event: Some(&event),
                            async_: None,
                        })
                        .unwrap();
                    assert_eq!(got, json!(format!("tag-{i}")));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn deep_nesting_is_bounded() {
    // Within limits: must evaluate without overflowing a 2MB test thread stack.
    let parens = format!("{}1{}", "(".repeat(60), ")".repeat(60));
    assert_eq!(run_with(&parens, None, None).unwrap(), json!(1));
    let lists = format!("{}1{}", "[".repeat(30), "]".repeat(30));
    assert!(run_with(&lists, None, None).is_ok());
    let calls = format!("{}1{}", "int(".repeat(60), ")".repeat(60));
    assert_eq!(run_with(&calls, None, None).unwrap(), json!(1));
    let sel = format!("event{}", ".a".repeat(150));
    assert_eq!(run_with(&sel, Some(&json!({})), None).unwrap(), Json::Null);
    let adds = vec!["1"; 150].join(" + ");
    assert_eq!(run_with(&adds, None, None).unwrap(), json!(150));
    // Long unary runs fold without recursion.
    assert_eq!(run_with(&format!("{}1", "-".repeat(90_000)), None, None).unwrap(), json!(1));
    let ors = vec!["false"; 5000].join(" || ");
    assert_eq!(run_with(&ors, None, None).unwrap(), json!(false));
    let ands = vec!["event.data.x == 1"; 2000].join(" && ");
    assert!(Expression::compile(&ands).is_ok());
    let nested_comp = format!("{}true{}", "[1].all(x, ".repeat(25), ")".repeat(25));
    assert_eq!(run_with(&nested_comp, None, None).unwrap(), json!(true));
    // Beyond limits: compile errors, never stack overflows.
    for src in [
        format!("{}1{}", "(".repeat(5000), ")".repeat(5000)),
        format!("{}1{}", "[".repeat(5000), "]".repeat(5000)),
        format!("{}1{}", "{1:".repeat(3000), "}".repeat(3000)),
        format!("event{}", ".a".repeat(50_000)),
        vec!["1"; 20_000].join(" + "),
        format!("{}x", "!".repeat(100_000)),
        format!("{}true{}", "[1].all(x, ".repeat(3000), ")".repeat(3000)),
        format!("{}1{}", "f(".repeat(3000), ")".repeat(3000)),
        format!("{}0{}", "true ? ".repeat(3000), " : 1".repeat(3000)),
        "a".repeat(200_000),
    ] {
        assert!(Expression::compile(&src).is_err(), "{}...", &src[..40.min(src.len())]);
    }
}

/// Tiny deterministic PRNG (xorshift) for fuzz-style tests.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn pick<'x, T>(&mut self, items: &'x [T]) -> &'x T {
        &items[(self.next() % items.len() as u64) as usize]
    }
}

#[test]
fn fuzz_no_panics() {
    let datasets: Json = serde_json::from_str(include_str!("../tests/fixtures/datasets.json")).unwrap();
    let data: Vec<&Json> = datasets.as_object().unwrap().values().collect();
    let corpus: Vec<String> = include_str!("../tests/fixtures/go_oracle.jsonl")
        .lines()
        .filter_map(|l| serde_json::from_str::<Json>(l).ok())
        .filter_map(|r| r["e"].as_str().map(str::to_string))
        .collect();
    let tokens = [
        "event", "async", "vars", "data", ".", "(", ")", "[", "]", "{", "}", ",", ":", "?", "!", "-", "+", "*",
        "/", "%", "==", "!=", "<", "<=", ">", ">=", "&&", "||", " in ", "1", "0", "1.5", "2u", "-1", "'a'",
        "\"b\"", "''", "null", "true", "false", "x", "has", "size", "exists", "all", "map", "filter",
        "exists_one", "matches", "'[a-'", "contains", "int", "uint", "double", "string", "bytes", "bool",
        "type", "dyn", "timestamp", "duration", "date", "now", "now_plus", "now_minus", "json_parse",
        "b64decode", "lowercase", "getHours", "'1h'", "'2020-01-01T00:00:00Z'", " ", "n", "list", "obj",
        "items", "tags", "nul", "missing", "\\", "'", "\"", "r'", "b'", "'''", "0x", "e9", "u", "`", "é",
        "9223372036854775807", "18446744073709551615u", "1e308", "//",
    ];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let check = |src: &str, rng: &mut Rng| {
        if let Ok(e) = Expression::compile(src) {
            let d = *rng.pick(&data);
            let vars = Vars {
                event: d.get("event"),
                async_: d.get("async"),
            };
            let _ = e.eval(vars);
            let _ = e.eval_bool(vars);
            let _ = e.eval_key(vars);
            let _ = e.eval_key_opt(vars);
            let _ = e.eval_int(vars);
            let _ = e.validate_restrictive();
            let _ = e.references_event();
        }
    };
    // Random token soup.
    for _ in 0..20_000 {
        let len = 1 + rng.next() % 14;
        let src: String = (0..len).map(|_| *rng.pick(&tokens)).collect();
        check(&src, &mut rng);
    }
    // Mutated corpus expressions.
    for _ in 0..20_000 {
        let mut src = rng.pick(&corpus).clone();
        for _ in 0..(1 + rng.next() % 3) {
            let pos = (rng.next() as usize) % (src.len() + 1);
            let pos = (0..=pos).rev().find(|p| src.is_char_boundary(*p)).unwrap_or(0);
            match rng.next() % 3 {
                0 => {
                    let tok: &&str = rng.pick(&tokens);
                    src.insert_str(pos, tok)
                }
                1 => {
                    let end = (pos + 1 + (rng.next() as usize) % 4).min(src.len());
                    let end = (end..=src.len()).find(|p| src.is_char_boundary(*p)).unwrap_or(src.len());
                    src.replace_range(pos..end, "");
                }
                _ => {
                    let other = rng.pick(&corpus).clone();
                    src.insert_str(pos, &other);
                }
            }
        }
        check(&src, &mut rng);
    }
    // Arbitrary bytes (valid UTF-8 only, as &str).
    for _ in 0..5_000 {
        let len = rng.next() % 40;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() % 128) as u8).collect();
        if let Ok(s) = String::from_utf8(bytes) {
            check(&s, &mut rng);
        }
    }
}

#[test]
fn weird_inputs() {
    // Non-object event values and odd JSON shapes.
    for event in [json!(null), json!("str"), json!(5), json!([1, 2]), json!({"data": [1]}), json!({"data": null})] {
        let vars = Vars {
            event: Some(&event),
            async_: Some(&event),
        };
        for src in [
            "event.data.x == 1",
            "event.data[0] == 1",
            "has(event.data.x)",
            "event == null",
            "size(event) > 0",
            "event.data.exists(x, x == 1)",
            "event.data + async.data",
        ] {
            let e = Expression::compile(src).unwrap();
            let _ = e.eval(vars);
            let _ = e.eval_bool(vars);
            let _ = e.eval_key(vars);
        }
    }
    let huge = json!({"data": {"n": 1e308, "neg": -1e308, "tiny": 5e-324}});
    let vars = Vars {
        event: Some(&huge),
        async_: None,
    };
    for src in [
        "event.data.n * event.data.n",
        "int(event.data.n)",
        "uint(event.data.neg)",
        "event.data.tiny / 0.0",
        "date(event.data.n)",
        "date(event.data.neg)",
        "[1,2][event.data.n]",
        "timestamp(9223372036854775807)",
        "timestamp('9999-12-31T23:59:59Z') + duration('1h')",
        "now_plus('2562047h')",
        "now_plus('9999999999h')",
        "duration('2562048h')",
        "-duration('1h') - duration('2562047h')",
        "string(event.data.tiny)",
        "event.data.n + ''",
    ] {
        let e = Expression::compile(src).unwrap();
        let _ = e.eval(vars);
        let _ = e.eval_key(vars);
    }
    assert_eq!(
        Expression::compile("string(event.data.tiny)").unwrap().eval(vars).unwrap(),
        json!("5e-324")
    );
}

#[test]
fn go_float_formatting() {
    let cases: &[(f64, &str)] = &[
        (0.0, "0"),
        (1.0, "1"),
        (1.5, "1.5"),
        (123456.0, "123456"),
        (1234567.0, "1.234567e+06"),
        (1e6, "1e+06"),
        (1e20, "1e+20"),
        (1e21, "1e+21"),
        (1e-4, "0.0001"),
        (1e-5, "1e-05"),
        (0.000123, "0.000123"),
        (100000.0, "100000"),
        (999999.0, "999999"),
        (1774512121538.0, "1.774512121538e+12"),
        (3.25, "3.25"),
        (-2.5e-7, "-2.5e-07"),
        (12345678.9, "1.23456789e+07"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
        (f64::NAN, "NaN"),
    ];
    for (f, want) in cases {
        assert_eq!(go_float(*f), *want, "{f}");
    }
}
