// Minimal `pi --mode rpc` stand-in for PiSessionRuntime wire tests.
//
// Replays REAL captured event lines (a byte-faithful golden transcript) after a
// prompt is accepted, and answers commands from a small JSON script, so the
// runtime is exercised against Pi's actual event shapes instead of invented
// ones. Stdlib only; framing is LF-only exactly like Pi's, never readline.
//
// Env:
//   T3_PI_MOCK_SCRIPT  JSON file: { responses: { <command>: { success, data? , error? } } }
//   T3_PI_MOCK_EVENTS  JSONL golden transcript; its non-response lines replay after `prompt`
//   T3_PI_MOCK_LOG     optional JSONL log of startup env/argv and every received command
import * as NodeFS from "node:fs";

const script = JSON.parse(NodeFS.readFileSync(process.env.T3_PI_MOCK_SCRIPT, "utf8"));
const readLines = (path) =>
  NodeFS.readFileSync(path, "utf8")
    .split("\n")
    .map((line) => (line.endsWith("\r") ? line.slice(0, -1) : line))
    .filter((line) => line.length > 0);

const replayLines = process.env.T3_PI_MOCK_EVENTS
  ? readLines(process.env.T3_PI_MOCK_EVENTS).filter((line) => {
      try {
        return JSON.parse(line).type !== "response";
      } catch {
        return false;
      }
    })
  : [];

/** Extra replays: `eventsAfter` maps a command to JSONL paths, whose non-response
 * lines are written after that command's response. */
const eventsFor = (command) => {
  const paths = script.eventsAfter?.[command];
  if (paths === undefined) return [];
  return (Array.isArray(paths) ? paths : [paths]).flatMap((path) =>
    readLines(path).filter((line) => {
      try {
        return JSON.parse(line).type !== "response";
      } catch {
        return false;
      }
    }),
  );
};

const log = (record) => {
  if (!process.env.T3_PI_MOCK_LOG) return;
  NodeFS.appendFileSync(process.env.T3_PI_MOCK_LOG, `${JSON.stringify(record)}\n`);
};

const write = (frame) => process.stdout.write(`${JSON.stringify(frame)}\n`);

log({
  kind: "start",
  argv: process.argv.slice(2),
  cwd: process.cwd(),
  // Only caller-injected markers: a Pi started by T3 must not inherit them.
  markers: Object.fromEntries(
    Object.entries(process.env).filter(([key]) => key.startsWith("PI_") || key === "AI_AGENT"),
  ),
});

const respond = (id, command, result) => {
  write({
    ...(id === undefined ? {} : { id }),
    type: "response",
    command,
    success: result?.success !== false,
    ...(result?.data === undefined ? {} : { data: result.data }),
    ...(result?.error === undefined ? {} : { error: result.error }),
  });
};

let buffer = "";

const handleLine = (line) => {
  let frame;
  try {
    frame = JSON.parse(line);
  } catch {
    respond(undefined, "parse", {
      success: false,
      error: "Failed to parse command: invalid JSON",
    });
    return;
  }
  const command = frame.type;
  log({ kind: "command", command, id: frame.id, frame });
  if (command === "extension_ui_response") return;

  const result = script.responses?.[command];
  if (result === undefined) {
    respond(frame.id, command, { success: false, error: `Unknown command: ${command}` });
    return;
  }
  // Pi answers `abort` only after the run settles, so tests can reproduce that
  // ordering by listing the command in `respondAfterEvents`.
  const respondAfterEvents = (script.respondAfterEvents ?? []).includes(command);
  if (!respondAfterEvents) respond(frame.id, command, result);
  const extra = eventsFor(command);
  for (const event of extra) process.stdout.write(`${event}\n`);
  if (command === "prompt") {
    for (const event of replayLines) process.stdout.write(`${event}\n`);
  }
  if (respondAfterEvents) respond(frame.id, command, result);
};

process.stdin.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  for (;;) {
    const index = buffer.indexOf("\n");
    if (index === -1) break;
    const raw = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
    if (line.length > 0) handleLine(line);
  }
});
// Pi exits cleanly on stdin EOF, so the runtime's close path is the real one.
process.stdin.on("end", () => process.exit(0));
