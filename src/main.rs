//! mcp-lazy — a lazy, optionally gated stdio proxy for MCP servers.
//!
//! Usage: mcp-lazy [--name NAME] [--gate] -- <server command> [args...]
//!
//! LAZY. Everything an MCP client asks while connecting is answered from an on-disk cache, so the
//! real server is not running while a session merely *could* use it:
//!   * the handshake — `initialize` (stateful revisions) or `server/discover` (2026-07-28 and
//!     later, stateless);
//!   * `tools/list`, `prompts/list`, `resources/list`, `resources/templates/list`, `ping`;
//!   * `subscriptions/listen` (2026-07-28): acknowledged locally and kept open.
//!
//! The first message the cache cannot answer (a `tools/call`, …) spawns the real server. On the
//! stateful revisions the client's `initialize` is replayed to it first (and its answer swallowed —
//! the client already has one); open listen subscriptions are replayed so the server can push its
//! own notifications (their duplicate acknowledgements are swallowed); buffered notifications follow.
//! From then on everything is forwarded except what the proxy keeps answering (lists, gate tools).
//!
//! GATE (`--gate`). The server's tools are not advertised at all — `tools/list` holds a single
//! `enable_tools` tool and the server's instructions are withheld — so an unused server costs the
//! client's context one line. Calling `enable_tools` exposes the full list (announced with
//! `notifications/tools/list_changed`) and returns the withheld instructions; `disable_tools` hides
//! it again. Neither call starts the server.
//!
//! The cache is filled by a one-off probe run of the real server, keyed by protocol version and
//! invalidated by a fingerprint of the command (size + mtime of every argument that is an existing
//! file). Anything unexpected falls back to running the server and forwarding — the worst case is
//! the memory the server costs without the proxy, never a broken call.

use serde_json::{Map, Value, json};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, UNIX_EPOCH};

/// Methods whose results are static for a given server build and can be served from the cache.
const CACHEABLE: &[&str] = &[
    "tools/list",
    "prompts/list",
    "resources/list",
    "resources/templates/list",
];
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const ENABLE_TOOL: &str = "enable_tools";
const DISABLE_TOOL: &str = "disable_tools";
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_SUBSCRIPTION: &str = "io.modelcontextprotocol/subscriptionId";
const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";
/// list_changed kinds a listen subscription can ask for: (filter key, capability, notification).
const LIST_KINDS: &[(&str, &str, &str)] = &[
    ("toolsListChanged", "tools", "notifications/tools/list_changed"),
    ("promptsListChanged", "prompts", "notifications/prompts/list_changed"),
    ("resourcesListChanged", "resources", "notifications/resources/list_changed"),
];

fn log(msg: &str) {
    eprintln!("[mcp-lazy] {msg}");
}

// ───────────────────────────── config ─────────────────────────────

struct Config {
    /// Cache key and the label used in the gate tools' descriptions.
    name: String,
    gate: bool,
    cmd: Vec<String>,
}

fn parse_args() -> Config {
    let mut args = std::env::args().skip(1);
    let (mut name, mut gate, mut cmd) = (None, false, Vec::new());
    while let Some(a) = args.next() {
        match a.as_str() {
            "--name" => name = args.next(),
            "--gate" => gate = true,
            "--" => {
                cmd.extend(args.by_ref());
                break;
            }
            _ => {
                cmd.push(a);
                cmd.extend(args.by_ref());
                break;
            }
        }
    }
    if cmd.is_empty() {
        eprintln!("usage: mcp-lazy [--name NAME] [--gate] -- <server command> [args...]");
        std::process::exit(2);
    }
    let name = name.unwrap_or_else(|| {
        let mut h = DefaultHasher::new();
        cmd.hash(&mut h);
        let base = Path::new(cmd.last().unwrap())
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        format!("{base}-{:016x}", h.finish())
    });
    Config { name: sanitize(&name), gate, cmd }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '.' { c } else { '_' })
        .collect()
}

// ───────────────────────────── protocol revisions ─────────────────────────────

/// How the client opened the connection, which decides the handshake method and how a version is read.
#[derive(Clone, Copy, PartialEq)]
enum Era {
    /// `initialize` + `notifications/initialized`; the server keeps connection state.
    Stateful,
    /// `server/discover`; every request carries its version in `_meta`, no connection state.
    Stateless,
}

impl Era {
    fn handshake(self) -> &'static str {
        match self {
            Era::Stateful => "initialize",
            Era::Stateless => "server/discover",
        }
    }
}

fn version_of(msg: &Value) -> Option<String> {
    msg.pointer("/params/protocolVersion")
        .or_else(|| msg.get("params")?.get("_meta")?.get(META_VERSION))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

// ───────────────────────────── cache ─────────────────────────────

/// Size + mtime of every argument that resolves to an existing file (plus the resolved program).
fn fingerprint(cmd: &[String]) -> String {
    let mut paths: Vec<PathBuf> = cmd.iter().map(PathBuf::from).collect();
    if let Some(prog) = which(&cmd[0]) {
        paths.push(prog);
    }
    let mut parts = Vec::new();
    for p in paths {
        let Ok(m) = fs::metadata(&p) else { continue };
        if !m.is_file() {
            continue;
        }
        let mt = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        parts.push(format!("{}:{}:{}", p.display(), m.len(), mt));
    }
    parts.join("|")
}

fn which(prog: &str) -> Option<PathBuf> {
    if prog.contains('/') {
        return Some(PathBuf::from(prog));
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|d| d.join(prog))
        .find(|p| p.is_file())
}

fn cache_path(name: &str) -> PathBuf {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    base.join("mcp-lazy").join(format!("{name}.json"))
}

/// Layout: { "fingerprint": "...", "versions": { "<version>": { "<method>": <response sans id> } } }
fn load_cache(path: &Path, fp: &str) -> Map<String, Value> {
    let Ok(text) = fs::read_to_string(path) else { return Map::new() };
    let Ok(Value::Object(root)) = serde_json::from_str::<Value>(&text) else { return Map::new() };
    if root.get("fingerprint").and_then(Value::as_str) != Some(fp) {
        return Map::new();
    }
    match root.get("versions") {
        Some(Value::Object(v)) => v.clone(),
        _ => Map::new(),
    }
}

/// Merge-and-write so sessions probing different versions don't drop each other's entries.
fn save_cache(path: &Path, fp: &str, version: &str, entry: &Map<String, Value>) {
    if let Some(dir) = path.parent() {
        let _ = fs::create_dir_all(dir);
    }
    let mut versions = load_cache(path, fp);
    versions.insert(version.to_owned(), Value::Object(entry.clone()));
    let body = json!({ "fingerprint": fp, "versions": versions });
    // Write-then-rename so concurrent sessions never read a half-written cache.
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    if fs::write(&tmp, body.to_string()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

// ───────────────────────────── io helpers ─────────────────────────────

fn spawn(cmd: &[String]) -> io::Result<Child> {
    Command::new(&cmd[0])
        .args(&cmd[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Reads the child's stdout lines on a thread so reads can time out.
fn line_channel(stdout: ChildStdout) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn write_line(w: &mut impl Write, line: &str) -> io::Result<()> {
    w.write_all(line.as_bytes())?;
    w.write_all(b"\n")?;
    w.flush()
}

fn emit(v: &Value) {
    let _ = write_line(&mut io::stdout().lock(), &v.to_string());
}

/// A response to `id` carrying the fields of `body` (`result` or `error`).
fn respond(id: &Value, body: &Value) {
    let mut m = Map::new();
    m.insert("jsonrpc".into(), json!("2.0"));
    m.insert("id".into(), id.clone());
    if let Value::Object(b) = body {
        for (k, v) in b {
            if k != "id" && k != "jsonrpc" {
                m.insert(k.clone(), v.clone());
            }
        }
    }
    emit(&Value::Object(m));
}

fn without_id(mut resp: Value) -> Value {
    if let Value::Object(m) = &mut resp {
        m.remove("id");
        m.remove("jsonrpc");
    }
    resp
}

fn is_response_to(v: &Value, id: &Value) -> bool {
    v.get("id") == Some(id) && v.get("method").is_none()
}

fn wait_for(rx: &mpsc::Receiver<String>, id: &Value) -> Option<Value> {
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        let left = deadline.checked_duration_since(Instant::now())?;
        let line = rx.recv_timeout(left).ok()?;
        if let Ok(v) = serde_json::from_str::<Value>(&line)
            && is_response_to(&v, id) {
                return Some(v);
            }
    }
}

/// Run the real server once, replay the client's handshake, collect the cacheable lists, kill it.
fn probe(cmd: &[String], era: Era, handshake_line: &str) -> Option<Map<String, Value>> {
    let mut child = spawn(cmd).ok()?;
    let mut stdin = child.stdin.take()?;
    let rx = line_channel(child.stdout.take()?);
    let result = (|| {
        let mut hs: Value = serde_json::from_str(handshake_line).ok()?;
        let hs_id = json!("mcp-lazy-probe-handshake");
        hs["id"] = hs_id.clone();
        write_line(&mut stdin, &hs.to_string()).ok()?;
        let hs_resp = wait_for(&rx, &hs_id)?;
        hs_resp.get("result")?;
        let mut entry = Map::new();
        entry.insert(era.handshake().into(), without_id(hs_resp));
        // Stateless requests each carry the client's _meta (version, client info); stateful ones
        // ride on the initialized connection.
        let params = match era {
            Era::Stateful => {
                write_line(&mut stdin, r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                    .ok()?;
                json!({})
            }
            Era::Stateless => json!({ "_meta": hs.pointer("/params/_meta").cloned().unwrap_or(json!({})) }),
        };
        for (i, method) in CACHEABLE.iter().enumerate() {
            let id = json!(format!("mcp-lazy-probe-{i}"));
            let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
            write_line(&mut stdin, &req.to_string()).ok()?;
            let resp = wait_for(&rx, &id)?;
            // A paginated list can't be served whole from the cache; leave it to the live server.
            if resp.pointer("/result/nextCursor").is_some_and(|c| !c.is_null()) {
                continue;
            }
            entry.insert((*method).into(), without_id(resp));
        }
        Some(entry)
    })();
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    result
}

// ───────────────────────────── proxy ─────────────────────────────

struct Live {
    child: Child,
    stdin: ChildStdin,
    out: JoinHandle<()>,
}

/// A `subscriptions/listen` the proxy acknowledged itself.
struct Listen {
    id: Value,
    honored: Map<String, Value>,
    line: String,
}

struct Proxy {
    cfg: Config,
    era: Option<Era>,
    version: String,
    /// Cached responses for the negotiated version; None → forward everything.
    entry: Option<Map<String, Value>>,
    /// The client's `initialize` (stateful), replayed to the server when it starts.
    init_line: Option<String>,
    listens: Vec<Listen>,
    /// Notifications received before the server ran, replayed to it on start.
    buffered: Vec<String>,
    /// Gate state: whether the server's own tools are currently advertised.
    exposed: bool,
    live: Option<Live>,
}

impl Proxy {
    fn new(cfg: Config) -> Self {
        let exposed = !cfg.gate;
        Proxy {
            cfg,
            era: None,
            version: String::new(),
            entry: None,
            init_line: None,
            listens: Vec::new(),
            buffered: Vec::new(),
            exposed,
            live: None,
        }
    }

    fn ensure_live(&mut self) -> &mut Live {
        if self.live.is_none() {
            let mut child = spawn(&self.cfg.cmd).unwrap_or_else(|e| {
                log(&format!("failed to spawn {:?}: {e}", self.cfg.cmd[0]));
                std::process::exit(1);
            });
            let mut stdin = child.stdin.take().unwrap();
            let rx = line_channel(child.stdout.take().unwrap());

            // Answers the client already got from us: the replayed initialize's response and the
            // acknowledgements of replayed listens.
            let mut skip_response = self
                .init_line
                .as_deref()
                .and_then(|l| serde_json::from_str::<Value>(l).ok())
                .and_then(|v| v.get("id").cloned());
            let mut skip_acks: Vec<Value> = self.listens.iter().map(|l| l.id.clone()).collect();

            if let Some(line) = &self.init_line {
                let _ = write_line(&mut stdin, line);
            }
            let out = thread::spawn(move || {
                let stdout = io::stdout();
                for line in rx {
                    if (skip_response.is_some() || !skip_acks.is_empty())
                        && let Ok(v) = serde_json::from_str::<Value>(&line) {
                            if skip_response.as_ref().is_some_and(|id| is_response_to(&v, id)) {
                                skip_response = None;
                                continue;
                            }
                            if v.get("method").and_then(Value::as_str)
                                == Some("notifications/subscriptions/acknowledged")
                            {
                                let sid = v.pointer("/params/_meta").and_then(|m| m.get(META_SUBSCRIPTION));
                                if let Some(pos) = skip_acks.iter().position(|id| Some(id) == sid) {
                                    skip_acks.remove(pos);
                                    continue;
                                }
                            }
                        }
                    if write_line(&mut stdout.lock(), &line).is_err() {
                        break;
                    }
                }
            });
            for l in &self.listens {
                let _ = write_line(&mut stdin, &l.line);
            }
            for line in self.buffered.drain(..) {
                let _ = write_line(&mut stdin, &line);
            }
            log("server started");
            self.live = Some(Live { child, stdin, out });
        }
        self.live.as_mut().unwrap()
    }

    fn forward(&mut self, line: &str) {
        let live = self.ensure_live();
        let _ = write_line(&mut live.stdin, line);
    }

    fn cached(&self, method: &str) -> Option<&Value> {
        self.entry.as_ref()?.get(method)
    }

    fn handshake_result(&self) -> Option<&Value> {
        self.cached(self.era?.handshake())?.get("result")
    }

    fn server_tools(&self) -> Option<&Vec<Value>> {
        self.cached("tools/list")?.pointer("/result/tools")?.as_array()
    }

    fn instructions(&self) -> Option<&str> {
        self.handshake_result()?.get("instructions")?.as_str()
    }

    /// Capabilities as advertised to the client (after the gate's edits).
    fn advertised_capabilities(&self) -> Value {
        let mut caps = self
            .handshake_result()
            .and_then(|r| r.get("capabilities"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        if self.cfg.gate {
            caps["tools"]["listChanged"] = json!(true);
            if caps.get("prompts").is_some() {
                caps["prompts"]["listChanged"] = json!(true);
            }
        }
        caps
    }

    fn gate_tool(&self, enable: bool) -> Value {
        let n = self.server_tools().map_or(0, Vec::len);
        let label = &self.cfg.name;
        let (name, description) = if enable {
            (
                ENABLE_TOOL,
                format!(
                    "The {label} MCP server's {n} tools are hidden to keep them out of context. Call \
                     this only when the task needs {label} — it exposes the tools (they appear once \
                     the tool list refreshes) and returns the server's usage instructions."
                ),
            )
        } else {
            (DISABLE_TOOL, format!("Hide the {label} MCP server's {n} tools again to free context."))
        };
        json!({
            "name": name,
            "description": description,
            "inputSchema": { "type": "object", "properties": {} },
            "annotations": { "readOnlyHint": true, "idempotentHint": true, "openWorldHint": false }
        })
    }

    /// First handshake: load or probe the cache for this version, then answer it.
    fn handle_handshake(&mut self, era: Era, id: &Value, msg: &Value, line: String) {
        let version = version_of(msg).unwrap_or_default();
        if self.entry.is_none() || self.version != version {
            let fp = fingerprint(&self.cfg.cmd);
            let cpath = cache_path(&self.cfg.name);
            let mut entry = load_cache(&cpath, &fp)
                .get(&version)
                .and_then(Value::as_object)
                .filter(|e| e.contains_key(era.handshake()))
                .cloned();
            if entry.is_none() {
                log(&format!("no cache for protocol {version:?}, probing the server once"));
                entry = probe(&self.cfg.cmd, era, &line);
                if let Some(e) = &entry {
                    save_cache(&cpath, &fp, &version, e);
                }
            }
            self.era = Some(era);
            self.version = version;
            self.entry = entry;
        }
        let Some(mut resp) = self.cached(era.handshake()).cloned() else {
            log("probe failed, forwarding everything to the server");
            self.entry = None;
            self.forward(&line);
            return;
        };
        if self.cfg.gate {
            let caps = self.advertised_capabilities();
            if let Some(r) = resp.get_mut("result").and_then(Value::as_object_mut) {
                r.remove("instructions"); // handed over by enable_tools
                r.insert("capabilities".into(), caps);
            }
        }
        respond(id, &resp);
        if era == Era::Stateful {
            self.init_line = Some(line);
        }
    }

    /// `subscriptions/listen`: acknowledge the honorable subset and keep the request open.
    fn handle_listen(&mut self, id: &Value, msg: &Value, line: String) {
        let requested = msg.pointer("/params/notifications").cloned().unwrap_or(json!({}));
        if requested.get("resourceSubscriptions").is_some_and(|r| r.as_array().is_some_and(|a| !a.is_empty())) {
            // Resource updates need the live server.
            self.forward(&line);
            return;
        }
        let caps = self.advertised_capabilities();
        let mut honored = Map::new();
        for (key, cap, _) in LIST_KINDS {
            if requested.get(*key) == Some(&json!(true)) && caps.pointer(&format!("/{cap}/listChanged")) == Some(&json!(true)) {
                honored.insert((*key).into(), json!(true));
            }
        }
        emit(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/subscriptions/acknowledged",
            "params": { "notifications": honored, "_meta": { META_SUBSCRIPTION: id } }
        }));
        if honored.is_empty() {
            // Nothing to deliver: close it the way the SDK does.
            let info = self.handshake_result().and_then(|r| r.pointer(&format!("/_meta/{}", META_SERVER_INFO.replace('/', "~1")))).cloned();
            respond(id, &json!({ "result": { "resultType": "complete", "_meta": { META_SUBSCRIPTION: id, META_SERVER_INFO: info } } }));
            return;
        }
        self.listens.push(Listen { id: id.clone(), honored, line });
    }

    fn handle_list(&mut self, id: &Value, method: &str, line: &str) {
        let Some(cached) = self.cached(method).cloned() else {
            self.forward(line);
            return;
        };
        if !self.cfg.gate {
            respond(id, &cached);
            return;
        }
        let mut resp = cached;
        match method {
            "tools/list" => {
                let mut tools = if self.exposed { self.server_tools().cloned().unwrap_or_default() } else { Vec::new() };
                tools.push(self.gate_tool(!self.exposed));
                resp["result"]["tools"] = Value::Array(tools);
            }
            "prompts/list" if !self.exposed => resp["result"]["prompts"] = json!([]),
            _ => {}
        }
        respond(id, &resp);
    }

    /// Tell the client its lists changed, in whichever form its revision expects.
    fn announce_list_changed(&self) {
        let kinds = LIST_KINDS.iter().filter(|(_, cap, _)| match *cap {
            "tools" => true,
            "prompts" => self.cached("prompts/list").is_some(),
            _ => false,
        });
        for (key, _, method) in kinds {
            match self.era {
                Some(Era::Stateless) => {
                    for l in self.listens.iter().filter(|l| l.honored.contains_key(*key)) {
                        emit(&json!({ "jsonrpc": "2.0", "method": method, "params": { "_meta": { META_SUBSCRIPTION: l.id } } }));
                    }
                }
                _ => emit(&json!({ "jsonrpc": "2.0", "method": method })),
            }
        }
    }

    fn handle_gate_call(&mut self, id: &Value, enable: bool) {
        self.exposed = enable;
        let n = self.server_tools().map_or(0, Vec::len);
        let label = &self.cfg.name;
        let text = if enable {
            let mut t = format!(
                "Exposed {n} {label} tools; they appear once the client refreshes its tool list \
                 (load their schemas via tool search if they are deferred)."
            );
            if let Some(i) = self.instructions() {
                t.push_str("\n\nServer instructions:\n");
                t.push_str(i);
            }
            t
        } else {
            format!("Hid {n} {label} tools.")
        };
        let mut body = json!({ "result": { "content": [{ "type": "text", "text": text }] } });
        if self.era == Some(Era::Stateless) {
            body["result"]["resultType"] = json!("complete");
        }
        respond(id, &body);
        self.announce_list_changed();
        log(if enable { "tools exposed" } else { "tools hidden" });
    }

    fn handle(&mut self, line: String) {
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            self.forward(&line);
            return;
        };
        let method = msg.get("method").and_then(Value::as_str).map(str::to_owned);
        let id = msg.get("id").cloned();
        let cached = self.entry.is_some();
        let idle = self.live.is_none();

        match (method.as_deref(), id) {
            (Some("initialize"), Some(id)) if idle && self.init_line.is_none() => {
                self.handle_handshake(Era::Stateful, &id, &msg, line)
            }
            (Some("server/discover"), Some(id)) if self.era != Some(Era::Stateful) => {
                self.handle_handshake(Era::Stateless, &id, &msg, line)
            }
            (Some("subscriptions/listen"), Some(id)) if cached && idle => self.handle_listen(&id, &msg, line),
            // Notifications are held until the server runs (notably notifications/initialized).
            (Some(_), None) if idle => self.buffered.push(line),
            (Some("ping"), Some(id)) if idle => respond(&id, &json!({ "result": {} })),
            (Some(m), Some(id))
                if cached && CACHEABLE.contains(&m) && msg.pointer("/params/cursor").is_none_or(Value::is_null) =>
            {
                self.handle_list(&id, m, &line)
            }
            (Some("tools/call"), Some(id)) if cached && self.cfg.gate => {
                match msg.pointer("/params/name").and_then(Value::as_str) {
                    Some(ENABLE_TOOL) => self.handle_gate_call(&id, true),
                    Some(DISABLE_TOOL) => self.handle_gate_call(&id, false),
                    _ => self.forward(&line),
                }
            }
            // Real work (tools/call, prompts/get, …), responses, anything unknown.
            _ => self.forward(&line),
        }
    }

    fn finish(self) -> ! {
        let Some(Live { mut child, stdin, out }) = self.live else { std::process::exit(0) };
        drop(stdin); // EOF to the server: it shuts down exactly as it would without the proxy.
        let status = child.wait();
        let _ = out.join();
        std::process::exit(status.ok().and_then(|s| s.code()).unwrap_or(0));
    }
}

fn main() {
    let mut proxy = Proxy::new(parse_args());
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if !line.trim().is_empty() {
            proxy.handle(line);
        }
    }
    proxy.finish();
}
