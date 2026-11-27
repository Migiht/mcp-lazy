//! mcp-lazy — a lazy, optionally gated stdio proxy for MCP servers.
//!
//! Usage: mcp-lazy [--name NAME] [--gate] [--per-cwd] [--idle SECONDS] -- <server command> [args...]
//!
//! IDLE (`--idle SECONDS`). Once the real server runs, it is stopped again after SECONDS with no
//! traffic in either direction and no request in flight; the next uncached message starts it again
//! (the handshake and listen subscriptions are replayed as on the first start). Without the flag
//! the server runs until the client disconnects.
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
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
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
    /// Cache file stem: the name, plus a hash of the working directory with `--per-cwd`.
    cache_key: String,
    gate: bool,
    /// Stop the running server after this long with no traffic (None = never).
    idle: Option<Duration>,
    cmd: Vec<String>,
}

const USAGE: &str =
    "usage: mcp-lazy [--name NAME] [--gate] [--per-cwd] [--idle SECONDS] -- <server command> [args...]";

fn parse_args() -> Config {
    let mut args = std::env::args().skip(1);
    let (mut name, mut gate, mut per_cwd, mut idle, mut cmd) = (None, false, false, None, Vec::new());
    while let Some(a) = args.next() {
        match a.as_str() {
            "--name" => name = args.next(),
            "--gate" => gate = true,
            "--per-cwd" => per_cwd = true,
            "--idle" => match args.next().and_then(|s| s.parse::<u64>().ok()) {
                Some(0) => idle = None,
                Some(s) => idle = Some(Duration::from_secs(s)),
                None => {
                    eprintln!("--idle needs a number of seconds\n{USAGE}");
                    std::process::exit(2);
                }
            },
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
        eprintln!("{USAGE}");
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
    let name = sanitize(&name);
    // Servers whose answers depend on where they run (instructions naming the project, tools
    // that differ per repository) need a cache per working directory.
    let cache_key = match std::env::current_dir() {
        Ok(cwd) if per_cwd => {
            let mut h = DefaultHasher::new();
            cwd.hash(&mut h);
            format!("{name}-cwd-{:016x}", h.finish())
        }
        _ => name.clone(),
    };
    Config { name, cache_key, gate, idle, cmd }
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
    if prog.contains('/') || prog.contains('\\') {
        return Some(PathBuf::from(prog));
    }
    let paths = std::env::var_os("PATH")?;
    // Windows: `npx` is `npx.cmd`, `uvx` is `uvx.exe` — try the executable extensions too.
    let exts: &[&str] = if cfg!(windows) { &["", ".exe", ".cmd", ".bat"] } else { &[""] };
    std::env::split_paths(&paths)
        .flat_map(|d| exts.iter().map(move |e| d.join(format!("{prog}{e}"))))
        .find(|p| p.is_file())
}

fn cache_path(name: &str) -> PathBuf {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .or_else(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from))
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
    // Windows: `Command` only finds `.exe` by bare name, so resolve `npx` → `npx.cmd` ourselves
    // (std runs `.cmd`/`.bat` through cmd.exe with safe argument quoting).
    let program = if cfg!(windows) && Path::new(&cmd[0]).extension().is_none() {
        which(&cmd[0]).map_or_else(|| cmd[0].clone().into(), |p| p.into_os_string())
    } else {
        cmd[0].clone().into()
    };
    Command::new(program)
        .args(&cmd[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Stop a server and everything it started (`npx`/`uvx` launchers keep the real server as a
/// grandchild, which would survive a plain kill on Windows).
fn kill_tree(child: &mut Child) {
    if cfg!(windows) {
        let _ = Command::new("taskkill")
            .args(["/T", "/F", "/PID", &child.id().to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
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
        let mut entry = Map::new();
        // A server that predates the stateless revision answers `server/discover` with "method not
        // found", and the client falls back to `initialize`. That answer is as stable as any other,
        // so it is cached too — otherwise every session of such a server would start it for real.
        if era == Era::Stateless && hs_resp.pointer("/error/code") == Some(&json!(-32601)) {
            entry.insert(era.handshake().into(), without_id(hs_resp));
            return Some(entry);
        }
        hs_resp.get("result")?;
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
    kill_tree(&mut child);
    result
}

// ───────────────────────────── proxy ─────────────────────────────

struct Live {
    child: Child,
    stdin: ChildStdin,
    out: JoinHandle<()>,
}

/// What the idle timer needs, shared with the thread that relays the server's output.
struct Activity {
    last: Instant,
    /// Requests still waiting for an answer: "c:<id>" client→server, "s:<id>" server→client.
    pending: HashSet<String>,
}

impl Activity {
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Activity { last: Instant::now(), pending: HashSet::new() }))
    }

    /// Record a message passing through. `from_client` decides which side a request belongs to.
    fn note(this: &Mutex<Self>, line: &str, from_client: bool) {
        let Ok(mut a) = this.lock() else { return };
        a.last = Instant::now();
        let Ok(v) = serde_json::from_str::<Value>(line) else { return };
        let Some(id) = v.get("id") else { return };
        let (mine, theirs) = if from_client { ("c:", "s:") } else { ("s:", "c:") };
        match v.get("method").and_then(Value::as_str) {
            // Listen subscriptions stay open for the whole session; they must not block idling.
            Some("subscriptions/listen") => {}
            Some(_) => {
                a.pending.insert(format!("{mine}{id}"));
            }
            None => {
                a.pending.remove(&format!("{theirs}{id}"));
            }
        }
    }
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
    /// The client's `notifications/initialized`, replayed after `initialize` on every restart.
    initialized_line: Option<String>,
    activity: Arc<Mutex<Activity>>,
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
            initialized_line: None,
            activity: Activity::new(),
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
            // On a restart after idling the server has to be told the handshake is complete again.
            if let Some(line) = &self.initialized_line {
                let _ = write_line(&mut stdin, line);
            }
            let activity = Arc::clone(&self.activity);
            let out = thread::spawn(move || {
                let stdout = io::stdout();
                for line in rx {
                    Activity::note(&activity, &line, false);
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
        Activity::note(&self.activity, line, true);
        let live = self.ensure_live();
        let _ = write_line(&mut live.stdin, line);
    }

    /// Called about once a second: stop the server if it has been quiet long enough.
    fn check_idle(&mut self) {
        let Some(limit) = self.cfg.idle else { return };
        // Without a cache the client's own handshake went straight to the server; a restart could
        // not replay it, so a server in forward-everything mode keeps running.
        if self.live.is_none() || self.entry.is_none() {
            return;
        }
        {
            let Ok(a) = self.activity.lock() else { return };
            if !a.pending.is_empty() || a.last.elapsed() < limit {
                return;
            }
        }
        self.stop_live();
        log(&format!("idle for {}s, server stopped (it restarts on the next call)", limit.as_secs()));
    }

    fn stop_live(&mut self) {
        let Some(Live { mut child, stdin, out }) = self.live.take() else { return };
        drop(stdin);
        kill_tree(&mut child);
        // The relay thread ends once the server's stdout closes; it is not joined so a stray
        // descendant holding the pipe can never block the proxy.
        drop(out);
        if let Ok(mut a) = self.activity.lock() {
            a.pending.clear();
        }
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
            let cpath = cache_path(&self.cfg.cache_key);
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
        if resp.get("error").is_some() {
            // The server refused this revision; the client will retry with another handshake.
            self.era = None;
            self.version.clear();
            self.entry = None;
            return;
        }
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
            // A listen opened while the server runs is forwarded, and remembered so a restart
            // after idling replays it (its duplicate acknowledgement is swallowed then).
            (Some("subscriptions/listen"), Some(id)) if cached => {
                self.listens.push(Listen { id, honored: Map::new(), line: line.clone() });
                self.forward(&line)
            }
            // Kept apart from the other notifications: it is replayed on every (re)start.
            (Some("notifications/initialized"), None) if idle => self.initialized_line = Some(line),
            // Notifications are held until the server runs.
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
        // ...but give up after a few seconds rather than leave a server (and its launcher's
        // children) running after the session is gone.
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break Some(s),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
                _ => {
                    kill_tree(&mut child);
                    break None;
                }
            }
        };
        if status.is_some() {
            let _ = out.join();
        }
        std::process::exit(status.and_then(|s| s.code()).unwrap_or(0));
    }
}

fn main() {
    let mut proxy = Proxy::new(parse_args());
    // stdin is read on its own thread so the idle timer keeps ticking while the client is quiet.
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    proxy.handle(line);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => proxy.check_idle(),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    proxy.finish();
}
