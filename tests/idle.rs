//! `--idle SECONDS`: a running server is stopped after a quiet spell and restarted on the next call,
//! with the handshake and listen subscriptions replayed so the client never notices.

mod common;

use common::*;
use serde_json::{Value, json};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

const SUB: &str = "io.modelcontextprotocol/subscriptionId";

fn idle_proxy(sb: &Sandbox, secs: &str) -> Proxy {
    sb.proxy_custom(&["--idle", secs], &[], &sb.work_dir())
}

fn whoami_pid(rev: Rev, p: &mut Proxy, id: u64) -> u64 {
    let who: Value = serde_json::from_str(&text_of(&rev.call(p, id, "whoami", json!({})))).unwrap();
    who["pid"].as_u64().unwrap()
}

#[test]
fn quiet_server_is_stopped_and_restarted_on_the_next_call() {
    for rev in REVS {
        let sb = Sandbox::new("idle");
        let mut p = idle_proxy(&sb, "1");
        rev.connect(&mut p);
        let probe_runs = sb.spawns().len();

        let first = whoami_pid(rev, &mut p, 1);
        assert_eq!(sb.spawns().len(), probe_runs + 1, "{rev:?}: started on first call");

        sleep(Duration::from_millis(2500));
        assert!(p.stderr.lock().unwrap().contains("server stopped"), "{rev:?}: not stopped while idle");

        // The next call starts a fresh server and still works.
        let second = whoami_pid(rev, &mut p, 2);
        assert_ne!(first, second, "{rev:?}: expected a new server process");
        assert_eq!(sb.spawns().len(), probe_runs + 2, "{rev:?}: exactly one restart");
        assert_eq!(text_of(&rev.call(&mut p, 3, "add", json!({ "a": 2, "b": 3 }))), "5", "{rev:?}");
        assert!(p.close().success(), "{rev:?}");
    }
}

#[test]
fn stateful_restart_replays_initialize_and_initialized() {
    // The server only knows the client's name if it received the client's own initialize again, and
    // the harness panics on any unexpected message, so this also proves the duplicate answer to the
    // replayed initialize is swallowed on every restart.
    let sb = Sandbox::new("idle-replay");
    let mut p = idle_proxy(&sb, "1");
    Rev::Stateful.connect(&mut p);
    for round in 0..2u64 {
        assert_eq!(text_of(&Rev::Stateful.call(&mut p, 10 + round, "client", json!({}))), "mcp-lazy-tests", "round {round}");
        assert_eq!(p.drain(Duration::from_millis(2500)), 0, "round {round}: stray messages");
    }
    assert!(p.stderr.lock().unwrap().matches("server started").count() >= 2, "restarted at least once");
}

#[test]
fn stateless_restart_replays_the_listen_without_a_second_acknowledgement() {
    let sb = Sandbox::new("idle-listen");
    let mut p = idle_proxy(&sb, "1");
    Rev::Stateless.connect(&mut p);
    p.drain(Duration::from_millis(300));
    let acks = p.take_notifications();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0]["params"]["_meta"][SUB], "listen:0");
    for round in 0..2u64 {
        Rev::Stateless.call(&mut p, 20 + round, "echo", json!({ "text": "x" }));
        p.drain(Duration::from_millis(2500));
        assert!(p.take_notifications().is_empty(), "round {round}: duplicate acknowledgement leaked");
    }
    assert!(p.stderr.lock().unwrap().contains("server stopped"));
}

#[test]
fn a_request_in_flight_keeps_the_server_running() {
    // The server asks the client for its roots mid-call. While that question (server → client) and
    // the call itself (client → server) are unanswered, the server must not be stopped, however long.
    let sb = Sandbox::new("idle-pending");
    let mut p = idle_proxy(&sb, "1");
    Rev::Stateful.connect(&mut p);
    p.send(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "roots", "arguments": {} } }));
    let ask = loop {
        let m = p.next_message();
        if m["method"] == "roots/list" {
            break m;
        }
    };
    sleep(Duration::from_millis(2500));
    assert!(!p.stderr.lock().unwrap().contains("server stopped"), "stopped with a request in flight");
    p.send(json!({ "jsonrpc": "2.0", "id": ask["id"], "result": { "roots": [{ "uri": "file:///project" }] } }));
    assert_eq!(text_of(&p.response(&json!(1))), r#"["file:///project"]"#);
}

#[test]
fn without_idle_the_server_keeps_running() {
    for flags in [&[][..], &["--idle", "0"][..]] {
        let sb = Sandbox::new("no-idle");
        let mut p = sb.proxy_custom(flags, &[], &sb.work_dir());
        Rev::Stateless.connect(&mut p);
        let first = whoami_pid(Rev::Stateless, &mut p, 1);
        sleep(Duration::from_millis(2500));
        assert_eq!(whoami_pid(Rev::Stateless, &mut p, 2), first, "{flags:?}: server was restarted");
        assert!(!p.stderr.lock().unwrap().contains("server stopped"), "{flags:?}");
    }
}

#[test]
fn bad_idle_value_is_rejected() {
    let sb = Sandbox::new("idle-bad");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mcp-lazy"))
        .args(["--idle", "soon", "--", "node", "x.mjs"])
        .current_dir(Path::new(&sb.dir))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--idle needs a number of seconds"));
}
