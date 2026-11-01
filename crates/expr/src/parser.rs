//! A small, recursion-bounded CEL parser.
//!
//! Produces a parse tree ([`PExpr`]) with the same shape cel-go's parser produces
//! (with macros left unexpanded): operators are calls named like `_+_`, `!_`,
//! `@in`, `_?_:_`; `a[b]` is an index node; `a.b` a select; `a.f(x)` a member
//! call. `&&`/`||` chains are balanced like cel-go does.

use std::sync::Arc;

/// Maximum nesting depth of the resulting tree. Keeps lowering/evaluation
/// recursion (and drop recursion) bounded.
pub(crate) const MAX_DEPTH: u32 = 200;
/// Maximum syntactic nesting (parentheses, brackets, call arguments, ternary
/// branches). cel-go allows 250; real expressions nest a handful of levels,
/// and this keeps parsing comfortably within small (2MB) thread stacks even in
/// debug builds.
pub(crate) const MAX_RECURSION: u32 = 64;
/// Maximum source length in characters (cel-go's default code point limit).
pub(crate) const MAX_SOURCE_CHARS: usize = 100_000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Lit {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
}

#[derive(Debug, Clone)]
pub(crate) struct PExpr {
    pub kind: PKind,
    pub depth: u32,
}

#[derive(Debug, Clone)]
pub(crate) enum PKind {
    Lit(Lit),
    Ident(String),
    Select(Box<PExpr>, String),
    Index(Box<PExpr>, Box<PExpr>),
    Call {
        name: String,
        target: Option<Box<PExpr>>,
        args: Vec<PExpr>,
    },
    List(Vec<PExpr>),
    Map(Vec<(PExpr, PExpr)>),
}

pub(crate) fn parse(src: &str) -> Result<PExpr, String> {
    if src.chars().count() > MAX_SOURCE_CHARS {
        return Err(format!(
            "expression code point size exceeds limit: size: {}, limit {}",
            src.chars().count(),
            MAX_SOURCE_CHARS
        ));
    }
    // Inngest lifts plain quoted string literals out of the expression before
    // handing it to cel-go, taking their contents verbatim: escape sequences are
    // *not* processed. It skips lifting when the source mentions `vars.`.
    let raw_plain = !src.contains("vars.");
    let toks = lex(src, raw_plain)?;
    let mut p = Parser {
        toks,
        i: 0,
        recursion: 0,
    };
    let e = p.expr()?;
    if !p.at_eof() {
        let t = p.peek();
        return Err(format!(
            "Syntax error: unexpected {} at offset {}",
            t.tok.describe(),
            t.pos
        ));
    }
    Ok(e)
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Integer literal magnitude (sign handled by the parser). Stored as u128 so
    /// that `-9223372036854775808` can be represented.
    Int(u128),
    Uint(u64),
    Double(f64),
    Str(String),
    Bytes(Vec<u8>),
    Ident(String),
    Op(&'static str),
    Eof,
}

impl Tok {
    fn describe(&self) -> String {
        match self {
            Tok::Int(i) => format!("integer '{i}'"),
            Tok::Uint(u) => format!("uint '{u}u'"),
            Tok::Double(d) => format!("double '{d}'"),
            Tok::Str(_) => "string literal".into(),
            Tok::Bytes(_) => "bytes literal".into(),
            Tok::Ident(s) => format!("identifier '{s}'"),
            Tok::Op(o) => format!("'{o}'"),
            Tok::Eof => "end of input".into(),
        }
    }
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    pos: usize,
}

const OPS: &[&str] = &[
    "==", "!=", "<=", ">=", "&&", "||", "<", ">", "!", "?", ":", "+", "-", "*", "/", "%", ".",
    ",", "[", "]", "(", ")", "{", "}",
];

fn lex(src: &str, raw_plain: bool) -> Result<Vec<Token>, String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        // Whitespace.
        if matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c) {
            i += 1;
            continue;
        }
        // Comments.
        if c == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        // Numbers (including `.5`).
        if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(|d| d.is_ascii_digit()))
        {
            let (tok, n) = lex_number(&src[i..]).map_err(|e| format!("{e} at offset {i}"))?;
            out.push(Token { tok, pos: start });
            i += n;
            continue;
        }
        // Identifiers, keywords and prefixed string literals.
        if c == b'_' || c.is_ascii_alphabetic() {
            let mut j = i;
            while j < b.len() && (b[j] == b'_' || b[j].is_ascii_alphanumeric()) {
                j += 1;
            }
            let word = &src[i..j];
            // String prefixes: r, b, rb, br (any case) directly followed by a quote.
            if j < b.len() && (b[j] == b'"' || b[j] == b'\'') && word.len() <= 2 {
                let lw = word.to_ascii_lowercase();
                let (raw, bytes) = match lw.as_str() {
                    "r" => (true, false),
                    "b" => (false, true),
                    "rb" | "br" => (true, true),
                    _ => (false, false),
                };
                if raw || bytes {
                    let (val, n) =
                        lex_string(&src[j..], raw, bytes).map_err(|e| format!("{e} at offset {j}"))?;
                    let tok = if bytes {
                        Tok::Bytes(val)
                    } else {
                        Tok::Str(String::from_utf8(val).map_err(|_| "invalid utf-8 in string")?)
                    };
                    out.push(Token { tok, pos: start });
                    i = j + n;
                    continue;
                }
            }
            out.push(Token {
                tok: Tok::Ident(word.to_string()),
                pos: start,
            });
            i = j;
            continue;
        }
        if c == b'"' || c == b'\'' {
            let triple = b.get(i + 1) == Some(&c) && b.get(i + 2) == Some(&c);
            let (val, n) = if raw_plain && !triple {
                lex_lifted_string(&src[i..]).map_err(|e| format!("{e} at offset {i}"))?
            } else {
                lex_string(&src[i..], false, false).map_err(|e| format!("{e} at offset {i}"))?
            };
            let s = String::from_utf8(val).map_err(|_| format!("invalid utf-8 in string at offset {i}"))?;
            out.push(Token {
                tok: Tok::Str(s),
                pos: start,
            });
            i += n;
            continue;
        }
        if c == b'`' {
            return Err(format!("Syntax error: unsupported syntax '`' at offset {i}"));
        }
        let mut matched = false;
        for op in OPS {
            if src[i..].starts_with(op) {
                out.push(Token {
                    tok: Tok::Op(op),
                    pos: start,
                });
                i += op.len();
                matched = true;
                break;
            }
        }
        if !matched {
            let ch = src[i..].chars().next().unwrap_or('?');
            return Err(format!("Syntax error: token recognition error at: '{ch}' (offset {i})"));
        }
    }
    out.push(Token {
        tok: Tok::Eof,
        pos: src.len(),
    });
    Ok(out)
}

/// Lexes a numeric literal; returns the token and the number of bytes consumed.
fn lex_number(s: &str) -> Result<(Tok, usize), String> {
    let b = s.as_bytes();
    // Hex integers.
    if b.len() > 2 && b[0] == b'0' && (b[1] == b'x' || b[1] == b'X') && b[2].is_ascii_hexdigit() {
        let mut j = 2;
        while j < b.len() && b[j].is_ascii_hexdigit() {
            j += 1;
        }
        let v = u128::from_str_radix(&s[2..j], 16).map_err(|_| "invalid int literal".to_string())?;
        if j < b.len() && (b[j] == b'u' || b[j] == b'U') {
            let u = u64::try_from(v).map_err(|_| "invalid uint literal".to_string())?;
            return Ok((Tok::Uint(u), j + 1));
        }
        if v > (i64::MAX as u128) + 1 {
            return Err("invalid int literal".into());
        }
        return Ok((Tok::Int(v), j));
    }
    let mut j = 0;
    while j < b.len() && b[j].is_ascii_digit() {
        j += 1;
    }
    let int_end = j;
    let mut is_float = false;
    // Fraction: requires digits on both sides of '.', except a leading '.'.
    if j < b.len() && b[j] == b'.' && b.get(j + 1).is_some_and(|d| d.is_ascii_digit()) {
        is_float = true;
        j += 1;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
    }
    // Exponent.
    if j < b.len() && (b[j] == b'e' || b[j] == b'E') {
        let mut k = j + 1;
        if k < b.len() && (b[k] == b'+' || b[k] == b'-') {
            k += 1;
        }
        if k < b.len() && b[k].is_ascii_digit() {
            while k < b.len() && b[k].is_ascii_digit() {
                k += 1;
            }
            is_float = true;
            j = k;
        }
    }
    if is_float {
        let v: f64 = s[..j].parse().map_err(|_| "invalid double literal".to_string())?;
        return Ok((Tok::Double(v), j));
    }
    let digits = &s[..int_end];
    if j < b.len() && (b[j] == b'u' || b[j] == b'U') {
        let u: u64 = digits.parse().map_err(|_| "invalid uint literal".to_string())?;
        return Ok((Tok::Uint(u), j + 1));
    }
    let v: u128 = digits.parse().map_err(|_| "invalid int literal".to_string())?;
    if v > (i64::MAX as u128) + 1 {
        return Err("invalid int literal".into());
    }
    Ok((Tok::Int(v), j))
}

/// Lexes a quoted string starting at `s[0]` (a quote). Returns the decoded
/// bytes and the number of bytes consumed.
fn lex_string(s: &str, raw: bool, bytes: bool) -> Result<(Vec<u8>, usize), String> {
    let b = s.as_bytes();
    let q = b[0];
    let triple = b.len() >= 3 && b[1] == q && b[2] == q;
    let mut i = if triple { 3 } else { 1 };
    let mut out: Vec<u8> = Vec::new();
    loop {
        if i >= b.len() {
            return Err("Syntax error: unterminated string literal".into());
        }
        let c = b[i];
        if triple {
            if c == q && b.get(i + 1) == Some(&q) && b.get(i + 2) == Some(&q) {
                return Ok((out, i + 3));
            }
        } else if c == q {
            return Ok((out, i + 1));
        } else if c == b'\n' || c == b'\r' {
            return Err("Syntax error: newline in string literal".into());
        }
        if c == b'\\' && !raw {
            let e = *b.get(i + 1).ok_or("Syntax error: unterminated escape sequence")?;
            i += 2;
            match e {
                b'\\' => out.push(b'\\'),
                b'\'' => out.push(b'\''),
                b'"' => out.push(b'"'),
                b'`' => out.push(b'`'),
                b'?' => out.push(b'?'),
                b'a' => out.push(0x07),
                b'b' => out.push(0x08),
                b'f' => out.push(0x0c),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'v' => out.push(0x0b),
                b'x' | b'X' => {
                    let h = s.get(i..i + 2).ok_or("Syntax error: invalid escape sequence")?;
                    let v = u8::from_str_radix(h, 16).map_err(|_| "Syntax error: invalid escape sequence")?;
                    push_code(&mut out, v as u32, bytes)?;
                    i += 2;
                }
                b'u' | b'U' => {
                    let n = if e == b'u' { 4 } else { 8 };
                    let h = s.get(i..i + n).ok_or("Syntax error: invalid escape sequence")?;
                    let v = u32::from_str_radix(h, 16).map_err(|_| "Syntax error: invalid escape sequence")?;
                    let ch = char::from_u32(v).ok_or("Syntax error: invalid unicode code point")?;
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    i += n;
                }
                b'0'..=b'3' => {
                    let h = s.get(i - 1..i + 2).ok_or("Syntax error: invalid escape sequence")?;
                    let v = u32::from_str_radix(h, 8).map_err(|_| "Syntax error: invalid escape sequence")?;
                    push_code(&mut out, v, bytes)?;
                    i += 2;
                }
                _ => return Err("Syntax error: invalid escape sequence".into()),
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
}

/// Lexes a plain quoted string the way Inngest's literal lifter does: the
/// content between the quotes is taken verbatim (a backslash only prevents the
/// following character from terminating the string), and newlines are allowed.
fn lex_lifted_string(s: &str) -> Result<(Vec<u8>, usize), String> {
    let b = s.as_bytes();
    let q = b[0];
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() {
            i += 2;
            continue;
        }
        if b[i] == q {
            return Ok((b[1..i].to_vec(), i + 1));
        }
        i += 1;
    }
    Err("Syntax error: unterminated string literal".into())
}

/// Pushes a `\x`/octal escape: a raw byte in bytes literals, a code point in strings.
fn push_code(out: &mut Vec<u8>, v: u32, bytes: bool) -> Result<(), String> {
    if bytes {
        out.push(v as u8);
    } else {
        let ch = char::from_u32(v).ok_or("Syntax error: invalid code point")?;
        let mut buf = [0u8; 4];
        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const RESERVED: &[&str] = &[
    "as", "break", "const", "continue", "else", "for", "function", "if", "import", "let", "loop",
    "package", "namespace", "return", "var", "void", "while", "in",
];

struct Parser {
    toks: Vec<Token>,
    i: usize,
    recursion: u32,
}

fn mk(kind: PKind) -> Result<PExpr, String> {
    let child = match &kind {
        PKind::Lit(_) | PKind::Ident(_) => 0,
        PKind::Select(e, _) => e.depth,
        PKind::Index(a, b) => a.depth.max(b.depth),
        PKind::Call { target, args, .. } => target
            .as_ref()
            .map(|t| t.depth)
            .into_iter()
            .chain(args.iter().map(|a| a.depth))
            .max()
            .unwrap_or(0),
        PKind::List(items) => items.iter().map(|a| a.depth).max().unwrap_or(0),
        PKind::Map(entries) => entries
            .iter()
            .map(|(k, v)| k.depth.max(v.depth))
            .max()
            .unwrap_or(0),
    };
    let depth = child + 1;
    if depth > MAX_DEPTH {
        return Err(format!(
            "expression nesting exceeds the maximum depth of {MAX_DEPTH}"
        ));
    }
    Ok(PExpr { kind, depth })
}

fn call(name: &str, args: Vec<PExpr>) -> Result<PExpr, String> {
    mk(PKind::Call {
        name: name.to_string(),
        target: None,
        args,
    })
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.toks[self.i.min(self.toks.len() - 1)]
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.i + n).min(self.toks.len() - 1)].tok
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek().tok, Tok::Eof)
    }

    fn is_op(&self, op: &str) -> bool {
        matches!(&self.peek().tok, Tok::Op(o) if *o == op)
    }

    fn eat(&mut self, op: &str) -> bool {
        if self.is_op(op) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn bump(&mut self) -> Token {
        let t = self.peek().clone();
        if self.i < self.toks.len() - 1 {
            self.i += 1;
        }
        t
    }

    fn err_here(&self, expected: &str) -> String {
        let t = self.peek();
        format!(
            "Syntax error: unexpected {} at offset {}, expecting {}",
            t.tok.describe(),
            t.pos,
            expected
        )
    }

    fn expect(&mut self, op: &str) -> Result<(), String> {
        if self.eat(op) {
            Ok(())
        } else {
            Err(self.err_here(&format!("'{op}'")))
        }
    }

    fn expr(&mut self) -> Result<PExpr, String> {
        self.recursion += 1;
        if self.recursion > MAX_RECURSION {
            return Err(format!(
                "expression nesting exceeds the maximum depth of {MAX_RECURSION}"
            ));
        }
        let cond = self.or()?;
        let res = if self.eat("?") {
            let t = self.or()?;
            self.expect(":")?;
            let f = self.expr()?;
            call("_?_:_", vec![cond, t, f])?
        } else {
            cond
        };
        self.recursion -= 1;
        Ok(res)
    }

    fn or(&mut self) -> Result<PExpr, String> {
        let mut terms = vec![self.and()?];
        while self.eat("||") {
            terms.push(self.and()?);
        }
        balance(terms, "_||_")
    }

    fn and(&mut self) -> Result<PExpr, String> {
        let mut terms = vec![self.relation()?];
        while self.eat("&&") {
            terms.push(self.relation()?);
        }
        balance(terms, "_&&_")
    }

    fn relation(&mut self) -> Result<PExpr, String> {
        let mut left = self.additive()?;
        loop {
            let op = match &self.peek().tok {
                Tok::Op("<") => "_<_",
                Tok::Op("<=") => "_<=_",
                Tok::Op(">") => "_>_",
                Tok::Op(">=") => "_>=_",
                Tok::Op("==") => "_==_",
                Tok::Op("!=") => "_!=_",
                Tok::Ident(s) if s == "in" => "@in",
                _ => break,
            };
            self.i += 1;
            let right = self.additive()?;
            left = call(op, vec![left, right])?;
        }
        Ok(left)
    }

    fn additive(&mut self) -> Result<PExpr, String> {
        let mut left = self.multiplicative()?;
        loop {
            let op = match &self.peek().tok {
                Tok::Op("+") => "_+_",
                Tok::Op("-") => "_-_",
                _ => break,
            };
            self.i += 1;
            let right = self.multiplicative()?;
            left = call(op, vec![left, right])?;
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> Result<PExpr, String> {
        let mut left = self.unary()?;
        loop {
            let op = match &self.peek().tok {
                Tok::Op("*") => "_*_",
                Tok::Op("/") => "_/_",
                Tok::Op("%") => "_%_",
                _ => break,
            };
            self.i += 1;
            let right = self.unary()?;
            left = call(op, vec![left, right])?;
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<PExpr, String> {
        if self.is_op("!") {
            let mut n = 0;
            while self.eat("!") {
                n += 1;
            }
            let operand = self.member()?;
            return if n % 2 == 1 {
                call("!_", vec![operand])
            } else {
                Ok(operand)
            };
        }
        if self.is_op("-") {
            // `-` directly followed by a numeric literal is a signed literal.
            if matches!(self.peek_at(1), Tok::Int(_) | Tok::Double(_)) {
                return self.member();
            }
            let mut n = 0;
            while self.eat("-") {
                n += 1;
            }
            let operand = self.member()?;
            return if n % 2 == 1 {
                call("-_", vec![operand])
            } else {
                Ok(operand)
            };
        }
        self.member()
    }

    fn member(&mut self) -> Result<PExpr, String> {
        let mut e = self.primary()?;
        loop {
            if self.eat(".") {
                if self.is_op("?") {
                    return Err(self.err_here("identifier (optional field selection is unsupported)"));
                }
                let name = self.ident()?;
                if self.eat("(") {
                    let args = self.expr_list(")")?;
                    e = mk(PKind::Call {
                        name,
                        target: Some(Box::new(e)),
                        args,
                    })?;
                } else {
                    e = mk(PKind::Select(Box::new(e), name))?;
                }
            } else if self.eat("[") {
                if self.is_op("?") {
                    return Err(self.err_here("expression (optional index is unsupported)"));
                }
                let idx = self.expr()?;
                self.expect("]")?;
                e = mk(PKind::Index(Box::new(e), Box::new(idx)))?;
            } else if self.is_op("{") {
                return Err(format!(
                    "Syntax error: message construction is unsupported (offset {})",
                    self.peek().pos
                ));
            } else {
                return Ok(e);
            }
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        match &self.peek().tok {
            Tok::Ident(s) => {
                if RESERVED.contains(&s.as_str()) {
                    return Err(format!(
                        "Syntax error: reserved identifier: {s} (offset {})",
                        self.peek().pos
                    ));
                }
                let s = s.clone();
                self.i += 1;
                Ok(s)
            }
            _ => Err(self.err_here("identifier")),
        }
    }

    /// Parses a comma-separated expression list terminated by `close`
    /// (trailing comma allowed for lists, not for call args — CEL allows it in
    /// neither for calls, but accepting it is harmless).
    fn expr_list(&mut self, close: &str) -> Result<Vec<PExpr>, String> {
        let mut items = Vec::new();
        if self.eat(close) {
            return Ok(items);
        }
        loop {
            items.push(self.expr()?);
            if self.eat(",") {
                if self.eat(close) {
                    return Ok(items);
                }
                continue;
            }
            self.expect(close)?;
            return Ok(items);
        }
    }

    fn primary(&mut self) -> Result<PExpr, String> {
        let t = self.bump();
        match t.tok {
            Tok::Op("-") => {
                // Signed numeric literal.
                let n = self.bump();
                match n.tok {
                    Tok::Int(v) => {
                        let v = if v == (i64::MAX as u128) + 1 {
                            i64::MIN
                        } else {
                            -(v as i64)
                        };
                        mk(PKind::Lit(Lit::Int(v)))
                    }
                    Tok::Double(d) => mk(PKind::Lit(Lit::Double(-d))),
                    _ => Err(format!("Syntax error: unexpected {} at offset {}", n.tok.describe(), n.pos)),
                }
            }
            Tok::Int(v) => {
                if v > i64::MAX as u128 {
                    return Err(format!("invalid int literal at offset {}", t.pos));
                }
                mk(PKind::Lit(Lit::Int(v as i64)))
            }
            Tok::Uint(u) => mk(PKind::Lit(Lit::Uint(u))),
            Tok::Double(d) => mk(PKind::Lit(Lit::Double(d))),
            Tok::Str(s) => mk(PKind::Lit(Lit::Str(Arc::from(s)))),
            Tok::Bytes(b) => mk(PKind::Lit(Lit::Bytes(Arc::from(b)))),
            Tok::Ident(name) => match name.as_str() {
                "true" => mk(PKind::Lit(Lit::Bool(true))),
                "false" => mk(PKind::Lit(Lit::Bool(false))),
                "null" => mk(PKind::Lit(Lit::Null)),
                _ => {
                    self.i -= 1;
                    let name = self.ident()?;
                    self.ident_or_call(name)
                }
            },
            Tok::Op(".") => {
                // Leading-dot (root-scoped) identifier.
                let name = self.ident()?;
                self.ident_or_call(name)
            }
            Tok::Op("(") => {
                let e = self.expr()?;
                self.expect(")")?;
                Ok(e)
            }
            Tok::Op("[") => {
                self.recursion += 1;
                let items = self.expr_list("]")?;
                self.recursion -= 1;
                mk(PKind::List(items))
            }
            Tok::Op("{") => {
                let mut entries = Vec::new();
                if !self.eat("}") {
                    loop {
                        if self.is_op("?") {
                            return Err(self.err_here("expression (optional entries are unsupported)"));
                        }
                        let k = self.expr()?;
                        self.expect(":")?;
                        let v = self.expr()?;
                        entries.push((k, v));
                        if self.eat(",") {
                            if self.eat("}") {
                                break;
                            }
                            continue;
                        }
                        self.expect("}")?;
                        break;
                    }
                }
                mk(PKind::Map(entries))
            }
            other => {
                self.i = self.i.saturating_sub(1);
                Err(format!(
                    "Syntax error: unexpected {} at offset {}",
                    other.describe(),
                    t.pos
                ))
            }
        }
    }

    fn ident_or_call(&mut self, name: String) -> Result<PExpr, String> {
        if self.eat("(") {
            let args = self.expr_list(")")?;
            return mk(PKind::Call {
                name,
                target: None,
                args,
            });
        }
        mk(PKind::Ident(name))
    }
}

/// Builds a balanced tree for associative logical operators, like cel-go.
fn balance(mut terms: Vec<PExpr>, op: &str) -> Result<PExpr, String> {
    if terms.len() == 1 {
        return Ok(terms.pop().expect("one term"));
    }
    let right = terms.split_off(terms.len() / 2);
    let l = balance(terms, op)?;
    let r = balance(right, op)?;
    call(op, vec![l, r])
}
