//! End-to-end tests of the proxy against a real MCP server (tests/fixture, official SDK), over both
//! protocol revisions: the stateful one (`initialize`) and the stateless one Claude Code speaks
//! (`server/discover` + `subscriptions/listen`).

mod common;

use common::*;
use serde_json::{Value, json};
use std::time::Duration;

const FIXTURE_TOOLS: [&str; 5] = ["echo", "add", "whoami", "client", "roots"];
const SUB: &str = "io.modelcontextprotocol/subscriptionId";

// ───────────────────────────── laziness ─────────────────────────────

#[test]
fn handshake_and_lists_are_served_from_cache_without_the_server() {
    for rev in REVS {
        let sb = Sandbox::new("cache");

        // First session: one probe run fills the cache, then the server is gone.
        let mut p = sb.proxy(false);
        let hs = rev.connect(&mut p);
        assert!(hs["result"]["instructions"].as_str().unwrap().contains("FIXTURE-INSTRUCTIONS"), "{rev:?}: {hs}");
        assert_eq!(rev.tool_names(&mut p, 1), FIXTURE_TOOLS, "{rev:?}");
        assert_eq!(sb.spawns().len(), 1, "{rev:?}: exactly the probe run");
        assert!(p.close().success());

        // Second session: everything from the cache, the server never starts.
        let mut p = sb.proxy(false);
        rev.connect(&mut p);
        assert_eq!(rev.tool_names(&mut p, 1), FIXTURE_TOOLS, "{rev:?}");
        let prompts = p.request(json!(2), "prompts/list", rev.params(json!({})));
        assert_eq!(prompts["result"]["prompts"][0]["name"], "greet", "{rev:?}");
        let pong = p.request(json!(3), "ping", rev.params(json!({})));
        assert_eq!(pong["result"], json!({}), "{rev:?}");
        assert!(p.close().success());
        assert_eq!(sb.spawns().len(), 1, "{rev:?}: cached sessions must not start the server");
    }
}

#[test]
fn cached_responses_take_the_clients_request_id() {
    let sb = Sandbox::new("ids");
    let mut p = sb.proxy(false);
    Rev::Stateless.connect(&mut p);
    for id in [json!(7), json!("abc"), json!(0)] {
        let r = p.request(id.clone(), "tools/list", Rev::Stateless.params(json!({})));
        assert_eq!(r["id"], id);
        assert_eq!(r["jsonrpc"], "2.0");
    }
}

#[test]
fn first_real_call_starts_the_server_once_and_forwards() {
    for rev in REVS {
        let sb = Sandbox::new("live");
        sb.proxy(false).close(); // warm nothing; just make sure an idle session spawns nothing
        assert_eq!(sb.spawns().len(), 0, "{rev:?}: a session that never connects spawns nothing");

        let mut p = sb.proxy(false);
        rev.connect(&mut p);
        rev.tool_names(&mut p, 1);
        let probe_runs = sb.spawns().len();

        assert_eq!(text_of(&rev.call(&mut p, 2, "echo", json!({ "text": "hi" }))), "echo: hi", "{rev:?}");
        assert_eq!(text_of(&rev.call(&mut p, 3, "add", json!({ "a": 2, "b": 3 }))), "5", "{rev:?}");
        assert_eq!(sb.spawns().len(), probe_runs + 1, "{rev:?}: one live server for the whole session");

        // Lists keep coming from the proxy after the server is up.
        assert_eq!(rev.tool_names(&mut p, 4), FIXTURE_TOOLS, "{rev:?}");
        assert!(p.close().success(), "{rev:?}");
    }
}

#[test]
fn live_server_inherits_the_proxys_working_directory() {
    let sb = Sandbox::new("cwd");
    let mut p = sb.proxy(false);
    Rev::Stateless.connect(&mut p);
    let who: Value = serde_json::from_str(&text_of(&Rev::Stateless.call(&mut p, 1, "whoami", json!({})))).unwrap();
    assert_eq!(who["cwd"].as_str().unwrap(), real_path(&sb.work_dir()));
}

#[test]
fn stateful_server_receives_the_replayed_initialize() {
    // The server only knows the client's name if it received the client's own initialize, so this
    // proves the replay — and the harness panics on any unexpected message, so it also proves the
    // server's duplicate initialize response was swallowed.
    let sb = Sandbox::new("replay");
    let mut p = sb.proxy(false);
    Rev::Stateful.connect(&mut p);
    assert_eq!(text_of(&Rev::Stateful.call(&mut p, 1, "client", json!({}))), "mcp-lazy-tests");
    assert_eq!(p.drain(Duration::from_millis(300)), 0, "no stray messages after going live");
}

#[test]
fn stateless_listen_is_acknowledged_locally_and_not_duplicated_when_live() {
    let sb = Sandbox::new("listen");
    let mut p = sb.proxy(false);
    Rev::Stateless.connect(&mut p);
    p.drain(Duration::from_millis(300));
    let acks = p.take_notifications();
    assert_eq!(methods(&acks), ["notifications/subscriptions/acknowledged"]);
    assert_eq!(acks[0]["params"]["_meta"][SUB], "listen:0");
    assert_eq!(acks[0]["params"]["notifications"], json!({ "toolsListChanged": true, "promptsListChanged": true }));

    // Going live replays the subscription to the server; its acknowledgement must not reach us twice.
    Rev::Stateless.call(&mut p, 1, "echo", json!({ "text": "x" }));
    p.drain(Duration::from_millis(500));
    assert!(p.take_notifications().is_empty(), "duplicate acknowledgement leaked");
}

#[test]
fn server_to_client_requests_are_routed_both_ways() {
    // The server asks the client for its roots mid-call; the client's answer must reach the server.
    let sb = Sandbox::new("roots");
    let mut p = sb.proxy(false);
    Rev::Stateful.connect(&mut p);
    p.send(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "roots", "arguments": {} } }));
    let ask = loop {
        let m = p.next_message();
        if m["method"] == "roots/list" {
            break m;
        }
    };
    p.send(json!({ "jsonrpc": "2.0", "id": ask["id"], "result": { "roots": [{ "uri": "file:///project" }] } }));
    let r = p.response(&json!(1));
    assert_eq!(text_of(&r), r#"["file:///project"]"#);
}

#[test]
fn a_server_without_the_stateless_revision_stays_lazy() {
    // Claude Code tries server/discover first; an older server refuses it and the client falls back
    // to initialize. Both answers must come from the cache once warm.
    let sb = Sandbox::new("legacy");
    let legacy = [("FIXTURE_LEGACY", "1")];
    for round in 0..2 {
        let mut p = sb.proxy_with(true, &legacy);
        let refused = p.request(json!("d"), "server/discover", Rev::Stateless.params(json!({})));
        assert_eq!(refused["error"]["code"], -32601, "round {round}: {refused}");
        let hs = Rev::Stateful.connect(&mut p);
        assert!(hs["result"].get("instructions").is_none(), "round {round}: gated");
        assert_eq!(Rev::Stateful.tool_names(&mut p, 1), ["enable_tools"], "round {round}");
        Rev::Stateful.call(&mut p, 2, "enable_tools", json!({}));
        assert_eq!(Rev::Stateful.tool_names(&mut p, 3).len(), FIXTURE_TOOLS.len() + 1, "round {round}");
        assert!(p.close().success());
    }
    assert_eq!(sb.spawns().len(), 2, "one probe per handshake kind, then nothing");

    // And it still works for real.
    let mut p = sb.proxy_with(true, &legacy);
    p.request(json!("d"), "server/discover", Rev::Stateless.params(json!({})));
    Rev::Stateful.connect(&mut p);
    assert_eq!(text_of(&Rev::Stateful.call(&mut p, 1, "echo", json!({ "text": "old" }))), "echo: old");
}

// ───────────────────────────── gate ─────────────────────────────

#[test]
fn gate_hides_tools_prompts_and_instructions() {
    for rev in REVS {
        let sb = Sandbox::new("gate-hide");
        let mut p = sb.proxy(true);
        let hs = rev.connect(&mut p);
        let result = &hs["result"];
        assert!(result.get("instructions").is_none(), "{rev:?}: instructions must be withheld");
        assert_eq!(result["capabilities"]["tools"]["listChanged"], true, "{rev:?}");
        assert_eq!(rev.tool_names(&mut p, 1), ["enable_tools"], "{rev:?}");
        let prompts = p.request(json!(2), "prompts/list", rev.params(json!({})));
        assert_eq!(prompts["result"]["prompts"], json!([]), "{rev:?}");
    }
}

#[test]
fn enable_and_disable_toggle_the_tool_list_without_starting_the_server() {
    for rev in REVS {
        let sb = Sandbox::new("gate-toggle");
        let mut p = sb.proxy(true);
        rev.connect(&mut p);
        rev.tool_names(&mut p, 1);
        p.drain(Duration::from_millis(200));
        p.take_notifications();
        let probe_runs = sb.spawns().len();

        let enabled = rev.call(&mut p, 2, "enable_tools", json!({}));
        let text = text_of(&enabled);
        assert!(text.contains("Exposed 5 fixture tools"), "{rev:?}: {text}");
        assert!(text.contains("FIXTURE-INSTRUCTIONS"), "{rev:?}: instructions are handed over on enable");
        p.drain(Duration::from_millis(300));
        let notes = p.take_notifications();
        assert_eq!(
            methods(&notes),
            ["notifications/tools/list_changed", "notifications/prompts/list_changed"],
            "{rev:?}"
        );
        if rev == Rev::Stateless {
            for n in &notes {
                assert_eq!(n["params"]["_meta"][SUB], "listen:0", "{rev:?}: stamped with the subscription");
            }
        }
        let mut exposed = FIXTURE_TOOLS.to_vec();
        exposed.push("disable_tools");
        assert_eq!(rev.tool_names(&mut p, 3), exposed, "{rev:?}");
        let prompts = p.request(json!(4), "prompts/list", rev.params(json!({})));
        assert_eq!(prompts["result"]["prompts"][0]["name"], "greet", "{rev:?}");

        rev.call(&mut p, 5, "disable_tools", json!({}));
        assert_eq!(rev.tool_names(&mut p, 6), ["enable_tools"], "{rev:?}");
        assert_eq!(sb.spawns().len(), probe_runs, "{rev:?}: toggling must not start the server");
    }
}

#[test]
fn gate_survives_going_live() {
    for rev in REVS {
        let sb = Sandbox::new("gate-live");
        let mut p = sb.proxy(true);
        rev.connect(&mut p);
        rev.call(&mut p, 1, "enable_tools", json!({}));
        assert_eq!(text_of(&rev.call(&mut p, 2, "echo", json!({ "text": "live" }))), "echo: live", "{rev:?}");
        // The proxy still owns the list and the gate tools once the server runs.
        assert!(rev.tool_names(&mut p, 3).contains(&"disable_tools".to_owned()), "{rev:?}");
        rev.call(&mut p, 4, "disable_tools", json!({}));
        assert_eq!(rev.tool_names(&mut p, 5), ["enable_tools"], "{rev:?}");
        assert!(p.close().success(), "{rev:?}");
    }
}

#[test]
fn hidden_tools_can_still_be_called_directly() {
    let sb = Sandbox::new("gate-direct");
    let mut p = sb.proxy(true);
    Rev::Stateless.connect(&mut p);
    let r = Rev::Stateless.call(&mut p, 1, "add", json!({ "a": 40, "b": 2 }));
    assert_eq!(text_of(&r), "42");
}

// ───────────────────────────── cache lifecycle ─────────────────────────────

#[test]
fn cache_is_rebuilt_when_the_server_changes() {
    let sb = Sandbox::new("fingerprint");
    for _ in 0..2 {
        let mut p = sb.proxy(false);
        Rev::Stateless.connect(&mut p);
        p.close();
    }
    assert_eq!(sb.spawns().len(), 1, "probed once, then cached");

    std::thread::sleep(Duration::from_millis(20)); // distinct mtime
    sb.write_entry("upgraded server");
    let mut p = sb.proxy(false);
    Rev::Stateless.connect(&mut p);
    p.close();
    assert_eq!(sb.spawns().len(), 2, "a changed server file invalidates the cache");
}

#[test]
fn each_protocol_version_is_cached_separately() {
    let sb = Sandbox::new("versions");
    for rev in REVS {
        let mut p = sb.proxy(false);
        rev.connect(&mut p);
        p.close();
    }
    assert_eq!(sb.spawns().len(), 2, "one probe per revision");
    for rev in REVS {
        let mut p = sb.proxy(false);
        rev.connect(&mut p);
        p.close();
    }
    assert_eq!(sb.spawns().len(), 2, "both revisions now cached");
}

#[test]
fn per_cwd_keeps_a_cache_per_working_directory() {
    let sb = Sandbox::new("per-cwd");
    let other = sb.dir.join("other");
    std::fs::create_dir_all(&other).unwrap();
    let session = |flags: &[&str], cwd: &std::path::Path| {
        let mut p = sb.proxy_custom(flags, &[], cwd);
        Rev::Stateless.connect(&mut p);
        let who = text_of(&Rev::Stateless.call(&mut p, 1, "whoami", json!({})));
        p.close();
        who
    };

    // Shared cache: the second directory reuses the first one's probe.
    session(&[], &sb.work_dir());
    session(&[], &other);
    assert_eq!(sb.spawns().len(), 3, "1 probe + 2 live");

    // Per-cwd cache: each directory probes once, then is served from its own cache.
    let before = sb.spawns().len();
    session(&["--per-cwd"], &sb.work_dir());
    session(&["--per-cwd"], &other);
    session(&["--per-cwd"], &other);
    assert_eq!(sb.spawns().len() - before, 2 + 3, "2 probes (one per directory) + 3 live");
    let probes_in_other = sb.spawns()[before..]
        .iter()
        .filter(|s| s.ends_with(&real_path(&other)))
        .count();
    assert_eq!(probes_in_other, 3, "1 probe + 2 live sessions ran in the other directory");
}

#[test]
fn a_corrupt_cache_is_ignored() {
    let sb = Sandbox::new("corrupt");
    let path = sb.cache_home().join("mcp-lazy/fixture.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "{ not json").unwrap();
    let mut p = sb.proxy(false);
    Rev::Stateless.connect(&mut p);
    assert_eq!(Rev::Stateless.tool_names(&mut p, 1), FIXTURE_TOOLS);
}

// ───────────────────────────── failure paths ─────────────────────────────

#[test]
fn a_server_that_cannot_start_is_reported_through_its_exit_code() {
    let sb = Sandbox::new("crash");
    let mut p = sb.proxy_with(false, &[("FIXTURE_CRASH", "1")]);
    Rev::Stateful.connect_raw(&mut p);
    let status = p.close();
    assert_eq!(status.code(), Some(3), "the server's own exit code is passed through");
}

#[test]
fn a_client_that_leaves_while_idle_ends_cleanly() {
    let sb = Sandbox::new("idle-exit");
    let mut p = sb.proxy(true);
    Rev::Stateless.connect(&mut p);
    assert!(p.close().success());
}

#[test]
fn bad_usage_is_rejected() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mcp-lazy")).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
}

trait ConnectRaw {
    fn connect_raw(self, p: &mut Proxy);
}

impl ConnectRaw for Rev {
    /// Send the handshake without waiting for an answer (for servers that never give one).
    fn connect_raw(self, p: &mut Proxy) {
        p.send(json!({
            "jsonrpc": "2.0", "id": "hs", "method": "initialize",
            "params": { "protocolVersion": STATEFUL, "capabilities": {}, "clientInfo": { "name": "t", "version": "0" } }
        }));
    }
}
