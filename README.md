# mcp-lazy

A tiny stdio proxy that makes MCP servers **lazy** and, optionally, **gated**.

MCP clients such as Claude Code start every configured server for every session, whether or not the
session ever uses it. A Node-based server easily costs ~100 MB per session, and a server with a
hundred tools puts a hundred tool names (plus its instructions) into the model's context.

`mcp-lazy` sits between the client and the server:

- **Lazy.** Everything the client asks while connecting — the handshake (`initialize` or
  `server/discover`), `tools/list`, `prompts/list`, `resources/list`,
  `resources/templates/list`, `ping`, `subscriptions/listen` — is answered from an on-disk cache.
  The real server starts on the first message the cache can't answer (a `tools/call`, …), once per
  session. An idle session costs a few MB instead of the server's footprint.
- **Gated** (`--gate`). The server's tools aren't advertised at all: the client sees a single
  `enable_tools` tool and the server instructions are withheld. Calling `enable_tools` exposes the
  full list (announced with `notifications/tools/list_changed`) and returns the instructions;
  `disable_tools` hides it again. Toggling never starts the server.

Both MCP wire revisions are supported: the stateful one (`initialize`) and the stateless
2026-07-28 one (`server/discover` + `subscriptions/listen`) that current Claude Code speaks. A server
that predates the stateless revision refuses `server/discover` and the client falls back to
`initialize`; both answers are cached, so such servers stay lazy too.

## Install

```sh
cargo install --git https://github.com/Migiht/mcp-lazy
```

## Use

Prefix the server command with `mcp-lazy [--name NAME] [--gate] --`:

```sh
claude mcp add-json figwright -s user '{
  "type": "stdio",
  "command": "mcp-lazy",
  "args": ["--name", "figwright", "--gate", "--", "node", "/path/to/figwright/dist/index.mjs"]
}'
```

- `--name` — cache key and the label used in the gate tools' descriptions (default: derived from the
  command).
- `--gate` — hide the tools until `enable_tools` is called.
- `--per-cwd` — keep a separate cache per working directory, for servers whose instructions or
  tools depend on the project they run in (e.g. a code indexer that reports the project's state).

Point it at the server's entry file rather than `npx pkg@latest`: the cache is invalidated by a
fingerprint (size + mtime) of the files named on the command line, so a pinned local build is what
lets it notice upgrades — and it saves the extra `npm` process.

## How it works

1. **Handshake from cache.** The cache lives in `$XDG_CACHE_HOME/mcp-lazy/<name>.json`, keyed by
   protocol version. On a miss the proxy runs the server once, replays the client's own handshake,
   records the list responses and kills it (a probe, ~0.3 s).
2. **Going live.** On the first uncached request the proxy starts the server in the same working
   directory and environment, replays the stateful `initialize` (swallowing the server's answer —
   the client already has one), replays open listen subscriptions (swallowing their duplicate
   acknowledgements) and buffered notifications, then forwards the request.
3. **Afterwards** everything is forwarded verbatim, except the lists and the gate tools, which the
   proxy keeps answering so the gate keeps working.
4. **Exit.** When the client closes stdin the proxy closes the server's stdin, waits for it and
   exits with its exit code.

Anything unexpected — an unknown protocol version, a probe that fails or times out, a paginated
list, a resource subscription — falls back to running the server and forwarding. The worst case is
the memory the server would cost without the proxy, never a broken call.

## Tests

```sh
cargo test --release                       # proxy tests against a real MCP server (official SDK)
MCP_LAZY_CLAUDE_E2E=1 cargo test --release --test claude_e2e -- --nocapture   # + real Claude Code
```

`tests/fixture` is a real MCP server built on `@modelcontextprotocol/server` (installed with
`npm ci` on first run; needs Node). `tests/proxy.rs` drives the proxy over both protocol revisions:
cache hits and probes, a single live start per session, working-directory inheritance,
`initialize` replay, server → client requests (`roots/list`), listen-subscription handling, servers
that only speak the stateful revision, the gate, per-directory caches, cache invalidation and
failure paths.
`tests/claude_e2e.rs` runs `claude -p` through `mcp-lazy --gate` and checks the stream-json
transcript: only `enable_tools` is visible at start, the model enables the gate, calls a real tool
and gets the server's answer — and the server starts only when it's needed. It's opt-in because it
uses the API.

## License

MIT
