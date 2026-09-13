// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";

import {
  assistantTextFromMessage,
  decodePiSessionState,
  decodePiSubagentSnapshot,
  decodePiToolExecutionEnd,
  isPiDialogMethod,
  parsePiFrame,
  PI_SUBAGENT_ASYNC_JSON_PREFIX,
  piModelSlug,
  piDialogCancelledResponse,
  piSubagentChildNodes,
  piSubagentSnapshotLine,
  splitPiModelSlug,
  splitPiLines,
  supportedThinkingLevels,
} from "./piRpcProtocol.ts";
import * as Option from "effect/Option";
import { describe, it } from "@effect/vitest";

const golden = (name: string): string =>
  NodeFS.readFileSync(new URL(`../testFixtures/${name}`, import.meta.url), "utf8");

describe("piRpcProtocol", () => {
  it("splits records on LF only, never on U+2028/U+2029", () => {
    // Pi emits these separators raw inside JSON strings; `node:readline` splits
    // on them and would corrupt the record, which is why the codec is manual.
    const record = JSON.stringify({
      type: "bash_execution_update",
      id: "4",
      delta: "A\u2028B\u2029C",
    });
    const framed = `${record}\n`;
    const { lines, rest } = splitPiLines(framed);
    NodeAssert.equal(rest, "");
    NodeAssert.deepEqual(lines, [record]);
    const parsed = parsePiFrame(lines[0] ?? "");
    NodeAssert.equal(parsed?.delta, "A\u2028B\u2029C");
    // The readline-style split breaks this record into four fragments (two
    // separators inside the string plus the record delimiter).
    NodeAssert.equal(framed.split(/\r\n|[\n\u2028\u2029]/u).length, 4);
  });

  it("keeps a partial record in the buffer until its newline arrives", () => {
    const first = splitPiLines('{"type":"agent_start"}');
    NodeAssert.deepEqual(first.lines, []);
    NodeAssert.equal(first.rest, '{"type":"agent_start"}');
    const second = splitPiLines(`${first.rest}\n`);
    NodeAssert.deepEqual(second.lines, ['{"type":"agent_start"}']);
  });

  it("tolerates CRLF and drops blank lines", () => {
    const { lines } = splitPiLines('{"a":1}\r\n\r\n{"b":2}\n');
    NodeAssert.deepEqual(lines, ['{"a":1}', '{"b":2}']);
  });

  it("rejects malformed, non-object, and typeless records", () => {
    NodeAssert.equal(parsePiFrame("{"), undefined);
    NodeAssert.equal(parsePiFrame("[]"), undefined);
    NodeAssert.equal(parsePiFrame('{"no_type":true}'), undefined);
    NodeAssert.equal(parsePiFrame('{"type":"agent_settled"}')?.type, "agent_settled");
  });

  it("decodes a real get_state payload from the captured transcript", () => {
    const stateFrame = golden("piGoldenNormalCompletion.jsonl")
      .split("\n")
      .map((line) => parsePiFrame(line))
      .find((frame) => frame?.type === "response" && frame.command === "get_state");
    NodeAssert.ok(stateFrame, "the golden transcript contains a get_state response");
    const decoded = decodePiSessionState(stateFrame.data);
    NodeAssert.ok(decoded._tag === "Some");
    NodeAssert.match(decoded.value.sessionId ?? "", /^[0-9a-f-]{36}$/u);
    NodeAssert.match(decoded.value.sessionFile ?? "", /\.jsonl$/u);
    NodeAssert.equal(decoded.value.thinkingLevel, "high");
    NodeAssert.equal(decoded.value.model?.provider, "opencode-go");
  });

  it("decodes a real tool execution end frame", () => {
    const toolEnd = golden("piGoldenNormalCompletion.jsonl")
      .split("\n")
      .map((line) => parsePiFrame(line))
      .find((frame) => frame?.type === "tool_execution_end");
    NodeAssert.ok(toolEnd);
    const decoded = decodePiToolExecutionEnd(toolEnd);
    NodeAssert.ok(decoded._tag === "Some");
    NodeAssert.equal(decoded.value.toolName, "bash");
    NodeAssert.equal(decoded.value.isError, false);
    NodeAssert.equal(decoded.value.toolCallId, toolEnd.toolCallId);
  });

  it("reports which extension UI methods block the agent", () => {
    NodeAssert.equal(isPiDialogMethod("select"), true);
    NodeAssert.equal(isPiDialogMethod("confirm"), true);
    NodeAssert.equal(isPiDialogMethod("input"), true);
    NodeAssert.equal(isPiDialogMethod("editor"), true);
    NodeAssert.equal(isPiDialogMethod("setWidget"), false);
    NodeAssert.equal(isPiDialogMethod("notify"), false);
    NodeAssert.deepEqual(piDialogCancelledResponse("abc"), {
      type: "extension_ui_response",
      id: "abc",
      cancelled: true,
    });
  });

  it("round-trips provider-qualified model slugs", () => {
    NodeAssert.equal(
      piModelSlug({ provider: "opencode-go", id: "glm-5.3" }),
      "opencode-go/glm-5.3",
    );
    NodeAssert.deepEqual(splitPiModelSlug("opencode-go/glm-5.3"), {
      provider: "opencode-go",
      modelId: "glm-5.3",
    });
    NodeAssert.equal(splitPiModelSlug("glm-5.3"), undefined);
    NodeAssert.equal(splitPiModelSlug("/glm-5.3"), undefined);
    NodeAssert.equal(splitPiModelSlug("opencode-go/"), undefined);
  });

  it("filters thinking levels a model does not support", () => {
    NodeAssert.deepEqual(
      supportedThinkingLevels(["off", "low", "high", "max"], {
        off: null,
        low: "low",
        high: "high",
        max: null,
      }),
      ["low", "high"],
    );
    // A model with no map keeps every advertised level.
    NodeAssert.deepEqual(supportedThinkingLevels(["low", "high"], undefined), ["low", "high"]);
  });

  it("reads assistant text from typed content blocks", () => {
    NodeAssert.equal(
      assistantTextFromMessage({
        role: "assistant",
        content: [
          { type: "thinking", thinking: "hmm" },
          { type: "text", text: "hello " },
          { type: "text", text: "world" },
        ],
      }),
      "hello world",
    );
    NodeAssert.equal(assistantTextFromMessage({ role: "assistant", content: [] }), undefined);
    NodeAssert.equal(assistantTextFromMessage("not a message"), undefined);
  });

  it("decodes the pi-subagents status snapshot out of the widget lines", () => {
    // Captured from a real run: a run with one step child, mid-flight.
    const line = `${PI_SUBAGENT_ASYNC_JSON_PREFIX}${JSON.stringify({
      kind: "pi-subagents.async-status-snapshot",
      version: 1,
      generatedAt: 1789315557314,
      caps: { maxRuns: 20, maxChildrenPerNode: 8, maxDepth: 3 },
      omitted: { runs: 0, children: 0, byteLimitExceeded: false },
      runs: [
        {
          id: "1fa67559-55fd-41e2-9fa2-4d7dc7852ed0",
          kind: "subagent",
          label: "scout",
          state: "running",
          startedAt: 1789315499035,
          activity: { currentTool: "bash", turnCount: 1, toolCount: 1 },
          children: [{ id: "step:0", kind: "step", label: "scout", state: "running" }],
        },
      ],
    })}`;
    const decoded = decodePiSubagentSnapshot(line);
    NodeAssert.ok(Option.isSome(decoded));
    const snapshot = Option.getOrThrow(decoded);
    NodeAssert.equal(snapshot.runs.length, 1);
    NodeAssert.equal(snapshot.omitted?.byteLimitExceeded, false);
    NodeAssert.equal(snapshot.runs[0]?.state, "running");
    NodeAssert.equal(snapshot.runs[0]?.activity?.currentTool, "bash");

    // Children decode lazily, and an unreadable child is dropped, not fatal.
    const children = piSubagentChildNodes(snapshot.runs[0] ?? { children: undefined });
    NodeAssert.equal(children.length, 1);
    NodeAssert.equal(children[0]?.id, "step:0");
    NodeAssert.deepEqual(piSubagentChildNodes({ children: ["not a node", { id: 1 }] }), []);
  });

  it("ignores status payloads it cannot read instead of failing the session", () => {
    // Malformed JSON, another extension's widget, and a future snapshot version.
    NodeAssert.equal(
      decodePiSubagentSnapshot("PI_SUBAGENT_ASYNC_JSON:{oops").pipe(Option.isNone),
      true,
    );
    NodeAssert.equal(
      decodePiSubagentSnapshot('SOME_OTHER_EXTENSION:{"runs":[]}').pipe(Option.isNone),
      true,
    );
    NodeAssert.equal(
      decodePiSubagentSnapshot(
        `${PI_SUBAGENT_ASYNC_JSON_PREFIX}${JSON.stringify({
          kind: "pi-subagents.async-status-snapshot",
          version: 2,
          runs: [],
        })}`,
      ).pipe(Option.isNone),
      true,
    );
    // A snapshot with no runs is valid, and means "nothing to show right now".
    const empty = decodePiSubagentSnapshot(
      `${PI_SUBAGENT_ASYNC_JSON_PREFIX}${JSON.stringify({
        kind: "pi-subagents.async-status-snapshot",
        version: 1,
        runs: [],
      })}`,
    );
    NodeAssert.ok(Option.isSome(empty));
    NodeAssert.deepEqual(Option.getOrThrow(empty).runs, []);

    // A lineless setWidget clears the widget: no snapshot line to read.
    NodeAssert.equal(piSubagentSnapshotLine(undefined), undefined);
    NodeAssert.equal(piSubagentSnapshotLine(["other:text"]), undefined);
  });
});
