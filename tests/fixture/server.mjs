// A real MCP server built on the official SDK, used as the proxied server in mcp-lazy's tests.
// serveStdio negotiates both the stateful (initialize) and stateless (server/discover) revisions,
// exactly as production servers do.
//
// Test hooks (environment):
//   FIXTURE_SPAWN_LOG  append "<pid> <cwd>" to this file on every start — lets tests count spawns
//   FIXTURE_CRASH=1    exit with code 3 before speaking — simulates a server that cannot start
import { appendFileSync } from 'node:fs';
import { McpServer } from '@modelcontextprotocol/server';
import { serveStdio } from '@modelcontextprotocol/server/stdio';
import { z } from 'zod';

if (process.env.FIXTURE_CRASH === '1') process.exit(3);
if (process.env.FIXTURE_SPAWN_LOG) {
  appendFileSync(process.env.FIXTURE_SPAWN_LOG, `${process.pid} ${process.cwd()}\n`);
}

const text = t => ({ content: [{ type: 'text', text: t }] });

const createServer = () => {
  const mcp = new McpServer(
    { name: 'fixture', version: '1.0.0' },
    { instructions: 'FIXTURE-INSTRUCTIONS: echo repeats text, add sums two numbers.' },
  );
  mcp.registerTool(
    'echo',
    { description: 'Return the given text prefixed with "echo: ".', inputSchema: z.object({ text: z.string() }) },
    async ({ text: t }) => text(`echo: ${t}`),
  );
  mcp.registerTool(
    'add',
    { description: 'Add two numbers.', inputSchema: z.object({ a: z.number(), b: z.number() }) },
    async ({ a, b }) => text(String(a + b)),
  );
  mcp.registerTool(
    'whoami',
    { description: 'Report the server process id and working directory.', inputSchema: z.object({}) },
    async () => text(JSON.stringify({ pid: process.pid, cwd: process.cwd() })),
  );
  // What the server learned about its client: from `initialize` on the stateful revision (so this
  // only answers correctly when the proxy replayed it), from each request's _meta on the stateless one.
  mcp.registerTool(
    'client',
    { description: 'Report the client name this server was told about.', inputSchema: z.object({}) },
    async (_args, ctx) =>
      text(String(mcp.server.getClientVersion()?.name ?? ctx?.mcpReq?._meta?.['io.modelcontextprotocol/clientInfo']?.name ?? 'unknown')),
  );
  mcp.registerPrompt(
    'greet',
    { description: 'Greet someone.', argsSchema: z.object({ name: z.string() }) },
    ({ name }) => ({ messages: [{ role: 'user', content: { type: 'text', text: `Hello, ${name}` } }] }),
  );
  return mcp;
};

serveStdio(createServer);
