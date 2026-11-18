//! Shared harness: a sandbox (own cache dir, spawn log, working dir) and a JSON-RPC client that
//! drives the real `mcp-lazy` binary in front of the real fixture MCP server (tests/fixture).
#![allow(dead_code)]

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Once, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const STATEFUL: &str = "2025-06-18";
pub const STATELESS: &str = "2026-07-28";
pub const TIMEOUT: Duration = Duration::from_secs(30);

pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixture")
}

/// Install the fixture's npm dependencies once per test binary.
pub fn ensure_fixture() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if fixture_dir().join("node_modules/@modelcontextprotocol/server").is_dir() {
            return;
        }
        let ok = Command::new("npm")
            .args(["ci", "--no-audit", "--no-fund"])
            .current_dir(fixture_dir())
            .status()
            .expect("npm is required to install the test fixture")
            .success();
        assert!(ok, "npm ci failed in tests/fixture");
    });
}

/// A per-test directory holding the proxy cache, the server spawn log and a server entry file.
pub struct Sandbox {
    pub dir: PathBuf,
}

impl Sandbox {
    pub fn new(tag: &str) -> Self {
        ensure_fixture();
        static N: AtomicUsize = AtomicUsize::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mcp-lazy-test-{tag}-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(dir.join("work")).unwrap();
        let sb = Sandbox { dir };
        sb.write_entry("");
        sb
    }

    /// The server entry the proxy runs: a one-line module importing the fixture. Rewriting it
    /// changes the proxy's command fingerprint, which is how tests simulate a server upgrade.
    pub fn entry(&self) -> PathBuf {
        self.dir.join("server-entry.mjs")
    }

    pub fn write_entry(&self, comment: &str) {
        let target = fixture_dir().join("server.mjs");
        std::fs::write(
            self.entry(),
            format!("// {comment}\nimport {:?};\n", target.to_string_lossy()),
        )
        .unwrap();
    }

    pub fn cache_home(&self) -> PathBuf {
        self.dir.join("cache")
    }

    pub fn spawn_log(&self) -> PathBuf {
        self.dir.join("spawns.log")
    }

    pub fn work_dir(&self) -> PathBuf {
        self.dir.join("work")
    }

    /// Every start of the real server, as "<pid> <cwd>" lines.
    pub fn spawns(&self) -> Vec<String> {
        std::fs::read_to_string(self.spawn_log())
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    pub fn proxy(&self, gate: bool) -> Proxy {
        self.proxy_with(gate, &[])
    }

    pub fn proxy_with(&self, gate: bool, env: &[(&str, &str)]) -> Proxy {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mcp-lazy"));
        cmd.args(["--name", "fixture"]);
        if gate {
            cmd.arg("--gate");
        }
        cmd.arg("--").arg("node").arg(self.entry());
        cmd.current_dir(self.work_dir())
            .env("XDG_CACHE_HOME", self.cache_home())
            .env("FIXTURE_SPAWN_LOG", self.spawn_log())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in env {
            cmd.env(k, v);
        }
        Proxy::start(cmd)
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A JSON-RPC session with a running `mcp-lazy`.
pub struct Proxy {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: mpsc::Receiver<Value>,
    /// Notifications received while waiting for responses, in arrival order.
    pub notifications: Vec<Value>,
    pub stderr: std::sync::Arc<std::sync::Mutex<String>>,
}

impl Proxy {
    fn start(mut cmd: Command) -> Self {
        let mut child = cmd.spawn().expect("spawn mcp-lazy");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let v: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("proxy wrote non-JSON line {line:?}: {e}"));
                if tx.send(v).is_err() {
                    break;
                }
            }
        });
        let stderr = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let sink = stderr.clone();
        let err = child.stderr.take().unwrap();
        thread::spawn(move || {
            for line in BufReader::new(err).lines().map_while(Result::ok) {
                let mut s = sink.lock().unwrap();
                s.push_str(&line);
                s.push('\n');
            }
        });
        Proxy { child, stdin, rx, notifications: Vec::new(), stderr }
    }

    pub fn send(&mut self, msg: Value) {
        let stdin = self.stdin.as_mut().expect("stdin already closed");
        writeln!(stdin, "{msg}").unwrap();
        stdin.flush().unwrap();
    }

    /// Send a request and wait for the response with the same id.
    pub fn request(&mut self, id: Value, method: &str, params: Value) -> Value {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        self.response(&id)
    }

    pub fn response(&mut self, id: &Value) -> Value {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let msg = self.rx.recv_timeout(left).unwrap_or_else(|_| {
                panic!("no response to id {id} (stderr: {})", self.stderr.lock().unwrap())
            });
            if msg.get("method").is_some() && msg.get("id").is_none() {
                self.notifications.push(msg);
            } else if msg.get("id") == Some(id) {
                return msg;
            } else {
                panic!("unexpected message while waiting for {id}: {msg}");
            }
        }
    }

    /// Collect whatever arrives within `wait` into `notifications`; returns how many arrived.
    pub fn drain(&mut self, wait: Duration) -> usize {
        let mut n = 0;
        while let Ok(msg) = self.rx.recv_timeout(wait) {
            assert!(msg.get("id").is_none() || msg.get("method").is_some(), "stray response: {msg}");
            self.notifications.push(msg);
            n += 1;
        }
        n
    }

    pub fn take_notifications(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.notifications)
    }

    /// Close stdin (the client going away) and wait for the proxy to exit.
    pub fn close(mut self) -> ExitStatus {
        drop(self.stdin.take());
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "proxy did not exit after stdin closed");
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ───────────── protocol helpers: one place that knows each revision's shapes ─────────────

/// Client side of a protocol revision.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Rev {
    Stateful,
    Stateless,
}

pub const REVS: [Rev; 2] = [Rev::Stateful, Rev::Stateless];

impl Rev {
    pub fn version(self) -> &'static str {
        match self {
            Rev::Stateful => STATEFUL,
            Rev::Stateless => STATELESS,
        }
    }

    /// Per-request params: stateless requests carry version and client info in `_meta`.
    pub fn params(self, mut p: Value) -> Value {
        if self == Rev::Stateless {
            p["_meta"] = json!({
                "io.modelcontextprotocol/protocolVersion": STATELESS,
                "io.modelcontextprotocol/clientInfo": { "name": "mcp-lazy-tests", "version": "0" },
                "io.modelcontextprotocol/clientCapabilities": {}
            });
        }
        p
    }

    /// Open a session the way a real client does; returns the handshake response.
    pub fn connect(self, p: &mut Proxy) -> Value {
        match self {
            Rev::Stateful => {
                let r = p.request(
                    json!("hs"),
                    "initialize",
                    json!({
                        "protocolVersion": STATEFUL,
                        "capabilities": {},
                        "clientInfo": { "name": "mcp-lazy-tests", "version": "0" }
                    }),
                );
                p.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
                r
            }
            Rev::Stateless => {
                let r = p.request(json!("hs"), "server/discover", self.params(json!({})));
                // Claude Code opens a list_changed subscription right after discovering.
                p.send(json!({
                    "jsonrpc": "2.0", "id": "listen:0", "method": "subscriptions/listen",
                    "params": self.params(json!({ "notifications": { "toolsListChanged": true, "promptsListChanged": true } }))
                }));
                r
            }
        }
    }

    pub fn tool_names(self, p: &mut Proxy, id: u64) -> Vec<String> {
        let r = p.request(json!(id), "tools/list", self.params(json!({})));
        r["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("bad tools/list response: {r}"))
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect()
    }

    pub fn call(self, p: &mut Proxy, id: u64, tool: &str, args: Value) -> Value {
        p.request(json!(id), "tools/call", self.params(json!({ "name": tool, "arguments": args })))
    }
}

pub fn text_of(resp: &Value) -> String {
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content in {resp}"))
        .to_owned()
}

pub fn methods(notes: &[Value]) -> Vec<String> {
    notes.iter().map(|n| n["method"].as_str().unwrap_or("").to_owned()).collect()
}
