//! End-to-end with the real Claude Code CLI: `claude -p` talks to the fixture MCP server through
//! `mcp-lazy --gate`, and the stream-json transcript is checked for what the model could see and do.
//!
//! It costs a few cents of API usage and needs a logged-in `claude` on PATH, so it is opt-in:
//!     MCP_LAZY_CLAUDE_E2E=1 cargo test --release --test claude_e2e -- --nocapture
//! Optional: MCP_LAZY_CLAUDE_MODEL (default "haiku").

mod common;

use common::*;
use serde_json::{Value, json};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn enabled() -> bool {
    if std::env::var("MCP_LAZY_CLAUDE_E2E").as_deref() == Ok("1") {
        return true;
    }
    eprintln!("skipped: set MCP_LAZY_CLAUDE_E2E=1 to run against the real Claude Code CLI");
    false
}

/// Every event of a `--output-format stream-json` run.
fn run_claude(sb: &Sandbox, prompt: &str, allowed: &[&str]) -> Vec<Value> {
    let config = json!({ "mcpServers": { "fixture": {
        "type": "stdio",
        "command": env!("CARGO_BIN_EXE_mcp-lazy"),
        "args": ["--name", "fixture", "--gate", "--", "node", sb.entry()],
        // The proxy's cache and the server's spawn log live in the sandbox, not the user's home.
        "env": {
            "XDG_CACHE_HOME": sb.cache_home(),
            "FIXTURE_SPAWN_LOG": sb.spawn_log(),
        }
    }}});
    let config_path = sb.dir.join("mcp.json");
    std::fs::write(&config_path, config.to_string()).unwrap();

    let model = std::env::var("MCP_LAZY_CLAUDE_MODEL").unwrap_or_else(|_| "haiku".into());
    let out = Command::new("claude")
        .arg("-p")
        .arg(prompt)
        .args(["--model", &model, "--strict-mcp-config", "--output-format", "stream-json", "--verbose"])
        .arg("--mcp-config")
        .arg(&config_path)
        .arg(format!("--allowedTools={}", allowed.join(",")))
        .current_dir(sb.work_dir())
        .stdin(std::process::Stdio::null())
        .output()
        .expect("`claude` must be on PATH");
    assert!(
        out.status.success(),
        "claude failed: {}\n{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn tool_uses(events: &[Value]) -> Vec<(String, Value)> {
    events
        .iter()
        .filter(|e| e["type"] == "assistant")
        .flat_map(|e| e["message"]["content"].as_array().cloned().unwrap_or_default())
        .filter(|c| c["type"] == "tool_use")
        .map(|c| (c["name"].as_str().unwrap_or("").to_owned(), c["input"].clone()))
        .collect()
}

fn tool_result_texts(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e["type"] == "user")
        .flat_map(|e| e["message"]["content"].as_array().cloned().unwrap_or_default())
        .filter(|c| c["type"] == "tool_result")
        .map(|c| match &c["content"] {
            Value::String(s) => s.clone(),
            Value::Array(parts) => parts.iter().filter_map(|p| p["text"].as_str()).collect::<Vec<_>>().join("\n"),
            other => other.to_string(),
        })
        .collect()
}

#[test]
fn claude_sees_only_the_gate_until_it_enables_then_calls_a_real_tool() {
    if !enabled() {
        return;
    }
    let sb = Sandbox::new("claude");
    let nonce = format!("nonce-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
    let prompt = format!(
        "This is an automated test. Do exactly this and nothing else: \
         1) call the tool mcp__fixture__enable_tools (load it with ToolSearch first if it is deferred); \
         2) then call mcp__fixture__echo with text \"{nonce}\" (load it with ToolSearch if needed); \
         3) reply with the echo result only."
    );
    let events = run_claude(
        &sb,
        &prompt,
        &["mcp__fixture__enable_tools", "mcp__fixture__echo", "ToolSearch"],
    );

    // At session start the model's tool list holds the gate and none of the server's tools.
    let init = events
        .iter()
        .find(|e| e["type"] == "system" && e["subtype"] == "init")
        .expect("no init event");
    let fixture_tools: Vec<&str> = init["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .filter(|t| t.starts_with("mcp__fixture__"))
        .collect();
    assert_eq!(fixture_tools, ["mcp__fixture__enable_tools"], "tools visible at start");
    let server = init["mcp_servers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "fixture")
        .expect("fixture server not reported");
    assert_eq!(server["status"], "connected", "{server}");

    // The model enabled the gate, then called the newly exposed tool with our nonce.
    let uses = tool_uses(&events);
    let names: Vec<&str> = uses.iter().map(|(n, _)| n.as_str()).collect();
    let enable_at = names.iter().position(|n| *n == "mcp__fixture__enable_tools").expect("enable_tools never called");
    let echo_at = names.iter().position(|n| *n == "mcp__fixture__echo").expect("echo never called");
    assert!(enable_at < echo_at, "echo before enable: {names:?}");
    assert_eq!(uses[echo_at].1["text"], nonce);

    let results = tool_result_texts(&events);
    assert!(
        results.iter().any(|t| t.contains("FIXTURE-INSTRUCTIONS")),
        "enable_tools did not hand over the server instructions: {results:?}"
    );
    assert!(
        results.iter().any(|t| t.contains(&format!("echo: {nonce}"))),
        "the real server's answer never reached the model: {results:?}"
    );

    // The server ran exactly twice: the one-off cache probe and the live server for the echo call.
    assert_eq!(sb.spawns().len(), 2, "server starts: {:?}", sb.spawns());
}

#[test]
fn claude_session_that_never_needs_the_server_never_starts_it() {
    if !enabled() {
        return;
    }
    let sb = Sandbox::new("claude-idle");
    // Warm the cache the way any earlier session would, so this run measures a steady state.
    let mut p = sb.proxy(true);
    Rev::Stateless.connect(&mut p);
    p.close();
    let warm = sb.spawns().len();

    let events = run_claude(&sb, "Reply with the single word: ok", &[]);
    assert!(events.iter().any(|e| e["type"] == "result"), "no result event");
    assert_eq!(sb.spawns().len(), warm, "an unused server was started: {:?}", sb.spawns());
}
