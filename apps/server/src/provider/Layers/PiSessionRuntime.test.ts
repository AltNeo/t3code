// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderEvent,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { buildPiProcessEnvironment } from "../Layers/PiProvider.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import {
  makePiSessionRuntime,
  piSubagentDescription,
  piTaskCompletionForState,
  piTaskStatusForState,
  type PiResumeCursor,
  type PiSessionRuntimeError,
  type PiSessionRuntimeShape,
} from "./PiSessionRuntime.ts";

const PEER_SOURCE = NodeFS.readFileSync(
  new URL("../testFixtures/piRpcMockPeer.mjs", import.meta.url),
  "utf8",
);
const NORMAL_COMPLETION = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../testFixtures/piGoldenNormalCompletion.jsonl",
);
const ABORT = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../testFixtures/piGoldenAbort.jsonl",
);
const BACKGROUND_CHILD = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../testFixtures/piGoldenBackgroundChild.jsonl",
);
const BACKGROUND_CHILD_ABORT = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../testFixtures/piGoldenBackgroundChildAbort.jsonl",
);
const FOREGROUND_CHILD = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../testFixtures/piGoldenForegroundChild.jsonl",
);

/** Real ids from the captures, so a fixture swap that changes them fails loudly. */
const BACKGROUND_RUN_ID = "1fa67559-55fd-41e2-9fa2-4d7dc7852ed0";
const BACKGROUND_TOOL_CALL_ID = "call_00_ZfV3WtinYRvw3HNfbEfN2652";
const FOREGROUND_TOOL_CALL_ID = "call_00_06GIgrPeeWUZDkLCTkh82312";

const THREAD = ThreadId.make("thread-pi-1");
const SESSION_ID = "01a09a71-4261-7189-9779-f4b9d29ff55c";
const SESSION_FILE = "/tmp/pi-mock-sessions/2026-09-13T11-05-08-962Z_session.jsonl";

interface PeerHarness {
  readonly binaryPath: string;
  readonly logPath: string;
  readonly received: (command: string) => ReadonlyArray<Readonly<Record<string, unknown>>>;
  readonly startRecord: () => Readonly<Record<string, unknown>> | undefined;
}

interface PeerOptions {
  readonly responses: Readonly<Record<string, unknown>>;
  readonly events?: string;
  readonly eventsAfter?: Readonly<Record<string, string | ReadonlyArray<string>>>;
  readonly respondAfterEvents?: ReadonlyArray<string>;
  readonly environment?: NodeJS.ProcessEnv;
}

const getStateData = (overrides?: Readonly<Record<string, unknown>>) => ({
  model: {
    id: "glm-5.3",
    name: "GLM-5.3",
    provider: "opencode-go",
    reasoning: true,
    contextWindow: 1_000_000,
    thinkingLevelMap: {
      off: null,
      minimal: null,
      low: "low",
      medium: null,
      high: "high",
      xhigh: null,
      max: "max",
    },
  },
  thinkingLevel: "high",
  isStreaming: false,
  isCompacting: false,
  sessionFile: SESSION_FILE,
  sessionId: SESSION_ID,
  messageCount: 4,
  pendingMessageCount: 0,
  ...overrides,
});

/** Writes a JSONL fixture in a temp dir; compaction events have no golden
 * transcript of their own, and the peer replays lines verbatim. */
const writeJsonl = (lines: ReadonlyArray<Readonly<Record<string, unknown>>>): string => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-events-"));
  const path = NodePath.join(directory, "events.jsonl");
  NodeFS.writeFileSync(path, lines.map((line) => `${JSON.stringify(line)}\n`).join(""), "utf8");
  return path;
};

const COMPACTION_START = { type: "compaction_start", reason: "manual" };
const compactionEnd = (
  overrides?: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => ({
  type: "compaction_end",
  reason: "manual",
  aborted: false,
  willRetry: false,
  ...overrides,
});

const subagentRun = (
  state: string,
  overrides?: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => ({
  id: BACKGROUND_RUN_ID,
  kind: "subagent",
  label: "scout",
  state,
  ...overrides,
});

/** `setWidget` frame carrying a pi-subagents status snapshot. */
const subagentWidget = (
  runs: ReadonlyArray<Readonly<Record<string, unknown>>>,
  omitted?: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => ({
  type: "extension_ui_request",
  id: "widget-1",
  method: "setWidget",
  widgetKey: "subagent-async",
  widgetLines: [
    `PI_SUBAGENT_ASYNC_JSON:${JSON.stringify({
      kind: "pi-subagents.async-status-snapshot",
      version: 1,
      generatedAt: 1,
      ...(omitted !== undefined ? { omitted } : {}),
      runs,
    })}`,
  ],
});

const payloadOf = (event: ProviderEvent): Readonly<Record<string, unknown>> =>
  (event.payload ?? {}) as Readonly<Record<string, unknown>>;

const makeFakeCli = (options: PeerOptions): PeerHarness => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-mock-"));
  const scriptPath = NodePath.join(directory, "script.json");
  const logPath = NodePath.join(directory, "peer.log.jsonl");
  NodeFS.writeFileSync(
    scriptPath,
    JSON.stringify({
      responses: options.responses,
      ...(options.eventsAfter ? { eventsAfter: options.eventsAfter } : {}),
      ...(options.respondAfterEvents ? { respondAfterEvents: options.respondAfterEvents } : {}),
    }),
    "utf8",
  );
  const binaryPath = writeFakeCli({
    directory,
    name: "pi",
    source: PEER_SOURCE,
    env: {
      T3_PI_MOCK_SCRIPT: scriptPath,
      T3_PI_MOCK_LOG: logPath,
      ...(options.events ? { T3_PI_MOCK_EVENTS: options.events } : {}),
    },
  });
  const readLog = (): ReadonlyArray<Readonly<Record<string, unknown>>> =>
    NodeFS.existsSync(logPath)
      ? NodeFS.readFileSync(logPath, "utf8")
          .split("\n")
          .filter((line) => line.trim().length > 0)
          .map((line) => JSON.parse(line) as Readonly<Record<string, unknown>>)
      : [];
  return {
    binaryPath,
    logPath,
    received: (command) =>
      readLog()
        .filter((record) => record.kind === "command" && record.command === command)
        .map((record) => (record.frame ?? {}) as Readonly<Record<string, unknown>>),
    startRecord: () => readLog().find((record) => record.kind === "start"),
  };
};

const startRuntime = (
  harness: PeerHarness,
  options?: {
    readonly resumeCursor?: PiResumeCursor;
    readonly model?: string;
    readonly environment?: NodeJS.ProcessEnv;
  },
): Effect.Effect<PiSessionRuntimeShape, PiSessionRuntimeError, Scope.Scope> =>
  makePiSessionRuntime({
    threadId: THREAD,
    providerInstanceId: ProviderInstanceId.make("pi"),
    binaryPath: harness.binaryPath,
    cwd: NodeOS.tmpdir(),
    environment: options?.environment ?? buildPiProcessEnvironment(process.env),
    extendEnv: false,
    launchArgs: [],
    runtimeMode: "full-access",
    ...(options?.model ? { model: options.model } : {}),
    ...(options?.resumeCursor ? { resumeCursor: options.resumeCursor } : {}),
  }).pipe(Effect.provide(NodeServices.layer));

/** Drains runtime events until the predicate holds; a bounded count keeps a
 * missing event from hanging the test as a timeout. */
const collectUntil = (
  runtime: PiSessionRuntimeShape,
  isDone: (event: ProviderEvent) => boolean,
  limit = 400,
): Effect.Effect<ReadonlyArray<ProviderEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const collected: Array<ProviderEvent> = [];
    const done = yield* Queue.unbounded<ReadonlyArray<ProviderEvent>>();
    const fiber = yield* Stream.runForEach(runtime.events, (event) =>
      Effect.gen(function* () {
        collected.push(event);
        if (isDone(event) || collected.length >= limit) {
          yield* Queue.offer(done, [...collected]);
        }
      }),
    ).pipe(Effect.forkScoped);
    const events = yield* Queue.take(done);
    yield* Fiber.interrupt(fiber);
    return events;
  });

const isTurnTerminal = (event: ProviderEvent): boolean =>
  event.method === "turn/completed" ||
  event.method === "turn/aborted" ||
  event.method === "session/exited";

const collectUntilTerminal = (runtime: PiSessionRuntimeShape) =>
  collectUntil(runtime, isTurnTerminal);

const methods = (events: ReadonlyArray<ProviderEvent>): ReadonlyArray<string> =>
  events.map((event) => event.method);

it.layer(NodeServices.layer)("PiSessionRuntime", (it) => {
  it.effect("starts a session, reports Pi's live identity, and settles a turn once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
            get_available_models: { success: true, data: { models: [] } },
          },
          events: NORMAL_COMPLETION,
        });
        const runtime = yield* startRuntime(harness);

        const session = yield* runtime.getSession;
        NodeAssert.equal(session.provider, ProviderDriverKind.make("pi"));
        NodeAssert.equal(session.status, "ready");
        NodeAssert.deepEqual(session.resumeCursor, {
          version: 1,
          sessionId: SESSION_ID,
          sessionFile: SESSION_FILE,
        });
        // Pi's live model is what T3 publishes, not the slug T3 asked for.
        NodeAssert.equal(session.model, "opencode-go/glm-5.3");

        const started = yield* runtime.sendTurn({ text: "run the command" });
        const events = yield* collectUntilTerminal(runtime);
        const seen = methods(events);

        // One T3 turn per prompt, completed exactly once at agent_settled.
        NodeAssert.equal(seen.filter((method) => method === "turn/started").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "turn/completed").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "turn/aborted").length, 0);
        NodeAssert.ok(seen.includes("process/stderr") === false);

        // Real captured tool lifecycle: stable id, mapped item type, merged data.
        const startedItem = events.find((event) => event.method === "turn/item/started");
        const completedItem = events.find((event) => event.method === "turn/item/completed");
        NodeAssert.ok(startedItem);
        NodeAssert.ok(completedItem);
        NodeAssert.equal(startedItem.itemId, completedItem?.itemId);
        NodeAssert.equal(
          (startedItem.payload as { itemType?: string }).itemType,
          "command_execution",
        );
        NodeAssert.equal((completedItem?.payload as { status?: string }).status, "completed");
        NodeAssert.equal(startedItem.turnId, started.turnId);

        // Assistant text arrives as deltas carrying the T3 turn id.
        const deltas = events.filter((event) => event.method === "turn/assistant/delta");
        NodeAssert.ok(deltas.length > 5);
        NodeAssert.ok(deltas.every((event) => event.turnId === started.turnId));
        NodeAssert.match(deltas.map((event) => event.textDelta ?? "").join(""), /t3-golden-ok/u);

        yield* runtime.close;
      }),
    ),
  );

  it.effect("treats a user abort as turn.aborted, not a completion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
            abort: { success: true },
          },
          // The abort golden replays on `abort`, and Pi answers abort only after
          // the run settles — the ordering that used to produce double terminals.
          eventsAfter: { abort: ABORT },
          respondAfterEvents: ["abort"],
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "start something long" });
        yield* runtime.interruptTurn;
        const events = yield* collectUntilTerminal(runtime);
        const seen = methods(events);
        NodeAssert.equal(seen.filter((method) => method === "turn/aborted").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "turn/completed").length, 0);
        const aborted = events.find((event) => event.method === "turn/aborted");
        NodeAssert.equal((aborted?.payload as { reason?: string }).reason, "Interrupted by user.");
        yield* runtime.close;
      }),
    ),
  );

  it.effect("fails closed when Pi resumes a different session than the cursor names", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            // Pi reports success and a fresh session when the file is missing.
            get_state: {
              success: true,
              data: getStateData({
                sessionId: "unrelated-session",
                sessionFile: "/tmp/pi-mock-sessions/blank.jsonl",
                messageCount: 0,
              }),
            },
            prompt: { success: true },
          },
        });
        const exit = yield* startRuntime(harness, {
          resumeCursor: { version: 1, sessionId: SESSION_ID, sessionFile: SESSION_FILE },
        }).pipe(Effect.exit);
        NodeAssert.equal(exit._tag, "Failure");
        if (exit._tag === "Failure") {
          NodeAssert.ok(
            String(exit.cause).includes("PiResumeCursorError"),
            `expected a resume cursor error, got ${String(exit.cause)}`,
          );
        }
        // The spawn used `--session <file>` and did not re-apply a model.
        const start = harness.startRecord();
        const argv = (start?.argv ?? []) as ReadonlyArray<string>;
        NodeAssert.deepEqual(argv.slice(0, 4), ["--mode", "rpc", "--session", SESSION_FILE]);
        NodeAssert.ok(!argv.includes("--model"));
      }),
    ),
  );

  it.effect("passes a scrubbed environment through to the Pi child", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
        });
        const environment = buildPiProcessEnvironment(
          { ...process.env, PI_SUBAGENT_CHILD: "1", PI_SESSION_ID: "leaked", AI_AGENT: "pi" },
          "~/.pi/agent",
        );
        const runtime = yield* startRuntime(harness, { environment });
        yield* runtime.getSession;
        const markers = (harness.startRecord()?.markers ?? {}) as Readonly<Record<string, string>>;
        NodeAssert.deepEqual(Object.keys(markers), ["PI_CODING_AGENT_DIR"]);
        yield* runtime.close;
      }),
    ),
  );

  it.effect("applies set_model only when the requested model changes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
            set_model: {
              success: true,
              data: { id: "glm-5.3-flash", name: "GLM-5.3 Flash", provider: "opencode-go" },
            },
          },
          events: NORMAL_COMPLETION,
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "one", model: "opencode-go/glm-5.3-flash" });
        yield* collectUntilTerminal(runtime);
        yield* runtime.sendTurn({ text: "two", model: "opencode-go/glm-5.3-flash" });
        yield* collectUntilTerminal(runtime);
        yield* runtime.sendTurn({ text: "three", model: "anthropic/claude-fable-5" });
        yield* collectUntilTerminal(runtime);
        const setModelFrames = harness.received("set_model");
        // Two changes out of three turns: the repeated model is not re-applied.
        NodeAssert.equal(setModelFrames.length, 2);
        NodeAssert.equal(setModelFrames[0]?.type, "set_model");
        NodeAssert.equal(setModelFrames[0]?.provider, "opencode-go");
        NodeAssert.equal(setModelFrames[0]?.modelId, "glm-5.3-flash");
        NodeAssert.equal(setModelFrames[1]?.provider, "anthropic");
        NodeAssert.equal(setModelFrames[1]?.modelId, "claude-fable-5");
        yield* runtime.close;
      }),
    ),
  );

  it.effect("refuses a second prompt while a turn is still running", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
            abort: { success: true },
          },
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "hold the turn open" });
        const busy = yield* runtime.sendTurn({ text: "second prompt" }).pipe(Effect.exit);
        NodeAssert.equal(busy._tag, "Failure");
        if (busy._tag === "Failure") {
          NodeAssert.ok(String(busy.cause).includes("PiSessionBusyError"));
        }
        // Interrupting without a settle still produces a terminal event.
        yield* runtime.interruptTurn;
        yield* runtime.close;
      }),
    ),
  );

  it.effect("sends image attachments to Pi as base64 prompt images", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: NORMAL_COMPLETION,
        });
        const runtime = yield* startRuntime(harness);
        const data = Buffer.from([0x01, 0x02, 0x03]).toString("base64");
        yield* runtime.sendTurn({ images: [{ data, mimeType: "image/png" }] });

        const promptFrames = harness.received("prompt");
        NodeAssert.equal(promptFrames.length, 1);
        NodeAssert.deepEqual(promptFrames[0]?.images, [
          { type: "image", data, mimeType: "image/png" },
        ]);
        // Pi needs a message next to the images.
        NodeAssert.equal(promptFrames[0]?.message, "(see attached image)");
        yield* collectUntilTerminal(runtime);
        yield* runtime.close;
      }),
    ),
  );

  it.effect("renders a detached pi-subagents child as an agent row that outlives its turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: BACKGROUND_CHILD_ABORT,
        });
        const runtime = yield* startRuntime(harness);
        const started = yield* runtime.sendTurn({ text: "launch a background scout" });
        const events = yield* collectUntil(
          runtime,
          (event) => event.method === "task/completed",
          2000,
        );
        const taskEvents = events.filter((event) => event.method.startsWith("task/"));
        const startedEvent = taskEvents.find((event) => event.method === "task/started");
        NodeAssert.ok(startedEvent, "expected a task.started for the detached child");
        NodeAssert.equal(payloadOf(startedEvent).taskId, BACKGROUND_RUN_ID);
        NodeAssert.equal(payloadOf(startedEvent).title, "scout");
        // No agentId: that is what puts the row on the agents surface instead of
        // classifying it as background work.
        NodeAssert.equal(payloadOf(startedEvent).agentId, undefined);
        NodeAssert.equal(startedEvent.turnId, started.turnId);

        const progress = taskEvents.filter((event) => event.method === "task/progress");
        NodeAssert.ok(progress.length >= 1, "expected at least one progress tick");
        // This capture carries 81 snapshot frames (a ~1/s heartbeat through a
        // 60s child). The bridge folds them, so progress is reported on material
        // change only — a per-frame emit would put dozens of rows on the panel.
        NodeAssert.ok(
          progress.length <= 4,
          `expected a folded status stream, got ${progress.length} progress rows`,
        );
        for (const event of progress) {
          NodeAssert.ok(
            String(payloadOf(event).description ?? "").trim().length > 0,
            "task.progress descriptions must be non-empty",
          );
        }

        const completed = taskEvents.find((event) => event.method === "task/completed");
        NodeAssert.ok(completed, "expected the run to complete");
        NodeAssert.equal(payloadOf(completed).status, "completed");
        // The child finished after the turn did, so it is not filed under a turn
        // the user already saw end.
        NodeAssert.equal(completed.turnId, undefined);
        // The launch tool call is linked so the work log can fold its row away.
        NodeAssert.equal(payloadOf(completed).toolUseId, BACKGROUND_TOOL_CALL_ID);
        for (const event of taskEvents) {
          NodeAssert.ok(
            event.turnId === undefined || event.turnId === started.turnId,
            "a task row must never be attributed to another turn",
          );
        }

        // Replaying the same snapshots must not re-open a finished run.
        yield* runtime.sendTurn({ text: "replay the same snapshots" });
        const drain = yield* collectUntilTerminal(runtime);
        NodeAssert.equal(drain.filter((event) => event.method === "task/completed").length, 0);
        yield* runtime.close;
      }),
    ),
  );

  it.effect("drives a foreground child from its own tool call", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: FOREGROUND_CHILD,
        });
        const runtime = yield* startRuntime(harness);
        const started = yield* runtime.sendTurn({ text: "run a foreground scout" });
        const events = yield* collectUntil(
          runtime,
          (event) => event.method === "task/completed",
          2000,
        );
        const taskEvents = events.filter((event) => event.method.startsWith("task/"));
        const startedEvent = taskEvents.find((event) => event.method === "task/started");
        NodeAssert.ok(startedEvent, "expected a row for the foreground child");
        // No snapshot frames exist for foreground children, so the tool call id
        // is the run's identity (the Antigravity batch precedent).
        NodeAssert.equal(payloadOf(startedEvent).taskId, FOREGROUND_TOOL_CALL_ID);
        NodeAssert.equal(payloadOf(startedEvent).toolUseId, FOREGROUND_TOOL_CALL_ID);
        NodeAssert.equal(payloadOf(startedEvent).title, "scout");
        NodeAssert.equal(startedEvent.turnId, started.turnId);

        const completed = taskEvents.find((event) => event.method === "task/completed");
        NodeAssert.ok(completed);
        NodeAssert.equal(payloadOf(completed).status, "completed");
        // Pi's own final output becomes the row's result line.
        NodeAssert.equal(payloadOf(completed).summary, "pong");
        NodeAssert.equal(payloadOf(completed).model, "openai-codex/gpt-5.6-luna:high");
        yield* runtime.close;
      }),
    ),
  );

  it.effect(
    "keeps a detached child running after an abort and stops its row with the session",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const eventsPath = writeJsonl([
            {
              type: "tool_execution_start",
              toolCallId: "call-bg-1",
              toolName: "subagent",
              args: { agent: "scout", async: true },
            },
            subagentWidget([subagentRun("queued")]),
            {
              type: "tool_execution_end",
              toolCallId: "call-bg-1",
              toolName: "subagent",
              result: { content: [], details: { asyncId: BACKGROUND_RUN_ID } },
            },
          ]);
          const harness = makeFakeCli({
            responses: {
              get_state: { success: true, data: getStateData() },
              prompt: { success: true },
              abort: { success: true },
            },
            events: eventsPath,
          });
          const runtime = yield* startRuntime(harness);
          yield* runtime.sendTurn({ text: "detach a child" });
          yield* runtime.interruptTurn;
          const aborted = yield* collectUntil(runtime, (event) => event.method === "turn/aborted");
          NodeAssert.equal(aborted.filter((event) => event.method === "turn/aborted").length, 1);
          // Aborting the parent kills neither a detached child nor its row.
          NodeAssert.equal(aborted.filter((event) => event.method === "task/completed").length, 0);
          const startedEvent = aborted.find((event) => event.method === "task/started");
          NodeAssert.ok(startedEvent);
          NodeAssert.equal(payloadOf(startedEvent).toolUseId, "call-bg-1");

          // Closing the session is where the row is finally closed out, and the
          // event has to survive the queue shutdown that follows it.
          const collecting = yield* collectUntil(
            runtime,
            (event) => event.method === "task/completed",
          ).pipe(Effect.forkScoped);
          yield* runtime.close;
          const closing = yield* Fiber.join(collecting);
          const stopped = closing.find((event) => event.method === "task/completed");
          NodeAssert.ok(stopped, "expected the live row to be closed at teardown");
          NodeAssert.equal(payloadOf(stopped).status, "stopped");
          NodeAssert.equal(payloadOf(stopped).taskId, BACKGROUND_RUN_ID);
        }),
      ),
  );

  it.effect("stops a foreground child's row when its turn is aborted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const eventsPath = writeJsonl([
          {
            type: "tool_execution_start",
            toolCallId: "call-fg-1",
            toolName: "subagent",
            args: { agent: "scout", async: false },
          },
          {
            type: "tool_execution_update",
            toolCallId: "call-fg-1",
            toolName: "subagent",
            partialResult: {
              details: {
                progress: [{ index: 0, agent: "scout", status: "running", recentOutput: ["pong"] }],
              },
            },
          },
        ]);
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
            abort: { success: true },
          },
          events: eventsPath,
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "run a foreground child" });
        yield* runtime.interruptTurn;
        const events = yield* collectUntil(runtime, (event) => event.method === "turn/aborted");
        const taskEvents = events.filter((event) => event.method.startsWith("task/"));
        const startedEvent = taskEvents.find((event) => event.method === "task/started");
        NodeAssert.ok(startedEvent);
        NodeAssert.equal(payloadOf(startedEvent).taskId, "call-fg-1");
        // Progress carries the child's own output tail.
        const progress = taskEvents.find((event) => event.method === "task/progress");
        NodeAssert.ok(progress, "expected progress from the tool-call update");
        NodeAssert.equal(payloadOf(progress).description, "pong");
        // The child runs inside the tool call, so aborting the turn ends it.
        const stopped = taskEvents.find((event) => event.method === "task/completed");
        NodeAssert.ok(stopped, "expected the foreground row to be stopped");
        NodeAssert.equal(payloadOf(stopped).status, "stopped");
        yield* runtime.close;
      }),
    ),
  );

  it.effect("warns once when Pi truncates its subagent status snapshot", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const eventsPath = writeJsonl([
          subagentWidget([subagentRun("queued")], {
            runs: 3,
            children: 0,
            byteLimitExceeded: true,
          }),
          subagentWidget([subagentRun("running")], {
            runs: 3,
            children: 0,
            byteLimitExceeded: true,
          }),
          { type: "agent_settled" },
        ]);
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: eventsPath,
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "launch a fleet" });
        const events = yield* collectUntilTerminal(runtime);
        const seen = methods(events);
        // One warning per session, not one per heartbeat, and the visible run
        // still renders.
        NodeAssert.equal(seen.filter((method) => method === "runtime/warning").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "task/started").length, 1);
        const warning = events.find((event) => event.method === "runtime/warning");
        NodeAssert.match(warning?.message ?? "", /3 subagent run/u);
        yield* runtime.close;
      }),
    ),
  );

  it("maps every Pi run state onto the T3 task vocabulary", () => {
    // The wide status vocabulary has no "stopped"; only `task.completed` does.
    NodeAssert.deepEqual(
      [
        "queued",
        "running",
        "paused",
        "idle",
        "complete",
        "stopped",
        "failed",
        "partial",
        "rejected",
      ].map((state) => [state, piTaskStatusForState(state)]),
      [
        ["queued", "pending"],
        ["running", "running"],
        ["paused", "waiting"],
        ["idle", "idle"],
        ["complete", "completed"],
        ["stopped", "cancelled"],
        ["failed", "failed"],
        ["partial", "failed"],
        ["rejected", "failed"],
      ],
    );
    // A state this build does not know stays live: a newer Pi must not have a
    // running child reported as finished.
    NodeAssert.equal(piTaskStatusForState("future_state"), "running");
    NodeAssert.equal(piTaskCompletionForState("future_state"), undefined);
    NodeAssert.deepEqual(
      ["complete", "stopped", "failed", "partial", "rejected", "running", "paused", "idle"].map(
        (state) => [state, piTaskCompletionForState(state)],
      ),
      [
        ["complete", "completed"],
        ["stopped", "stopped"],
        ["failed", "failed"],
        ["partial", "failed"],
        ["rejected", "failed"],
        ["running", undefined],
        ["paused", undefined],
        ["idle", undefined],
      ],
    );

    // The status line is always usable, and a CI gate's own detail wins.
    NodeAssert.equal(
      piSubagentDescription({ id: "r", kind: "subagent", label: "scout", state: "running" }),
      "Running",
    );
    NodeAssert.equal(
      piSubagentDescription({
        id: "r",
        kind: "subagent",
        label: "scout",
        state: "running",
        activity: { currentTool: "bash" },
      }),
      "Running bash",
    );
    NodeAssert.equal(
      piSubagentDescription({
        id: "r",
        kind: "subagent",
        label: "scout",
        state: "running",
        activity: { state: "needs_attention" },
      }),
      "Needs attention",
    );
    NodeAssert.equal(
      piSubagentDescription({
        id: "r",
        kind: "subagent",
        label: "scout",
        state: "running",
        hostStep: { detail: "CI gate waiting on approval" },
      }),
      "CI gate waiting on approval",
    );
    NodeAssert.equal(
      piSubagentDescription({ id: "r", kind: "workflow", label: "fleet", state: "partial" }),
      "One or more child runs failed",
    );
  });

  it.effect("reports a paused or attention-needing child as waiting, not running", () =>
    Effect.scoped(
      Effect.gen(function* () {
        // Two runs so one snapshot exercises the resting and terminal branches
        // of the same fold.
        const pausedRun = { id: "run-paused", kind: "subagent", label: "auditor", state: "queued" };
        const attentionRun = {
          id: "run-attention",
          kind: "subagent",
          label: "builder",
          state: "queued",
        };
        const eventsPath = writeJsonl([
          subagentWidget([pausedRun, attentionRun]),
          subagentWidget([
            { ...pausedRun, state: "paused" },
            { ...attentionRun, state: "running", activity: { state: "needs_attention" } },
          ]),
          subagentWidget([
            { ...pausedRun, state: "partial" },
            { ...attentionRun, state: "stopped" },
          ]),
          { type: "agent_settled" },
        ]);
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: eventsPath,
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "run two children" });
        const events = yield* collectUntilTerminal(runtime);

        const forTask = (taskId: string) =>
          events.filter((event) => payloadOf(event).taskId === taskId);
        const paused = forTask("run-paused");
        // A resting run is a status patch, never a progress tick or a terminal.
        NodeAssert.deepEqual(
          paused.map((event) => event.method),
          ["task/started", "task/updated", "task/completed"],
        );
        const pausedUpdated = paused[1];
        const pausedCompleted = paused[2];
        NodeAssert.ok(pausedUpdated, "expected the paused run to be reported as waiting");
        NodeAssert.ok(pausedCompleted, "expected the partial run to finish");
        NodeAssert.equal(payloadOf(pausedUpdated).status, "waiting");
        NodeAssert.equal(payloadOf(pausedCompleted).status, "failed");
        NodeAssert.equal(payloadOf(pausedCompleted).summary, "One or more child runs failed");

        const attention = forTask("run-attention");
        NodeAssert.deepEqual(
          attention.map((event) => event.method),
          ["task/started", "task/updated", "task/completed"],
        );
        const attentionUpdated = attention[1];
        const attentionCompleted = attention[2];
        NodeAssert.ok(attentionUpdated, "expected the attention signal to be reported");
        NodeAssert.ok(attentionCompleted, "expected the stopped run to finish");
        NodeAssert.equal(payloadOf(attentionUpdated).status, "waiting");
        NodeAssert.equal(payloadOf(attentionUpdated).description, "Needs attention");
        NodeAssert.equal(payloadOf(attentionCompleted).status, "stopped");
        yield* runtime.close;
      }),
    ),
  );

  it.effect("reports compaction only when Pi confirms it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        // Pi emits compaction_end before it answers the command, so the runtime
        // cannot decide from the response alone.
        const successHarness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            compact: { success: true, data: { summary: "s" } },
          },
          eventsAfter: { compact: writeJsonl([COMPACTION_START, compactionEnd()]) },
          respondAfterEvents: ["compact"],
        });
        const runtime = yield* startRuntime(successHarness);
        const collecting = yield* collectUntil(
          runtime,
          (event) => event.method === "thread/compaction/completed",
        ).pipe(Effect.forkScoped);
        yield* runtime.compactThread;
        const events = yield* Fiber.join(collecting);
        const seen = methods(events);
        // `session/started` was already queued; the point is that compaction is
        // reported once and no turn lifecycle appears around it.
        NodeAssert.equal(seen.at(-1), "thread/compaction/completed");
        NodeAssert.equal(
          seen.filter((method) => method === "thread/compaction/completed").length,
          1,
        );
        NodeAssert.deepEqual(
          seen.filter((method) => method.startsWith("turn/")),
          ["turn/compaction/started"],
        );
        yield* runtime.close;

        // A failed compaction is a typed failure, never a completed one.
        const failureHarness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            compact: { success: false, error: "Nothing to compact (session too small)" },
          },
          eventsAfter: {
            compact: writeJsonl([
              COMPACTION_START,
              compactionEnd({
                errorMessage: "Compaction failed: Nothing to compact (session too small)",
              }),
            ]),
          },
          respondAfterEvents: ["compact"],
        });
        const failing = yield* startRuntime(failureHarness);
        const failed = yield* failing.compactThread.pipe(Effect.exit);
        NodeAssert.equal(failed._tag, "Failure");
        if (failed._tag === "Failure") {
          NodeAssert.ok(String(failed.cause).includes("Nothing to compact"));
        }
        yield* failing.close;
      }),
    ),
  );

  it.effect("refuses to report a compaction that Pi aborts after accepting it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        // Pi answers `compact` successfully, then reports the compaction aborted:
        // the response is not evidence that a compaction happened.
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            compact: { success: true, data: { summary: "s" } },
            prompt: { success: true },
          },
          events: NORMAL_COMPLETION,
          eventsAfter: {
            compact: writeJsonl([COMPACTION_START, compactionEnd({ aborted: true })]),
          },
          respondAfterEvents: ["compact"],
        });
        const runtime = yield* startRuntime(harness);
        const settled = yield* runtime.compactThread.pipe(Effect.exit);
        NodeAssert.equal(settled._tag, "Failure");
        if (settled._tag === "Failure") {
          NodeAssert.ok(
            String(settled.cause).includes("Pi aborted the compaction"),
            `expected an abort error, got ${String(settled.cause)}`,
          );
        }

        // The events queue is unbounded and FIFO, so the turn below drains the
        // compaction events too: a completion would have to show up here.
        yield* runtime.sendTurn({ text: "after the aborted compaction" });
        const events = yield* collectUntilTerminal(runtime);
        const seen = methods(events);
        NodeAssert.equal(
          seen.filter((method) => method === "thread/compaction/completed").length,
          0,
        );
        NodeAssert.equal(seen.filter((method) => method === "runtime/warning").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "turn/completed").length, 1);
        yield* runtime.close;
      }),
    ),
  );

  it.effect("refuses to report a compaction that fails after Pi accepts it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        // The command response is `success:true`, but `compaction_end` carries
        // an error: the outcome event, not the response, decides. Without this
        // guard a failed summarizer would still mark the thread compacted.
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            compact: { success: true, data: { summary: "s" } },
            prompt: { success: true },
          },
          events: NORMAL_COMPLETION,
          eventsAfter: {
            compact: writeJsonl([
              COMPACTION_START,
              compactionEnd({
                errorMessage: "Compaction failed: summarizer unavailable",
              }),
            ]),
          },
          respondAfterEvents: ["compact"],
        });
        const runtime = yield* startRuntime(harness);
        const failed = yield* runtime.compactThread.pipe(Effect.exit);
        NodeAssert.equal(failed._tag, "Failure");
        if (failed._tag === "Failure") {
          NodeAssert.ok(
            String(failed.cause).includes("summarizer unavailable"),
            `expected the compaction_end error, got ${String(failed.cause)}`,
          );
        }

        // The events queue is unbounded and FIFO, so the turn below drains the
        // compaction events too: a completion would have to show up here.
        yield* runtime.sendTurn({ text: "after the failed compaction" });
        const events = yield* collectUntilTerminal(runtime);
        const seen = methods(events);
        NodeAssert.equal(
          seen.filter((method) => method === "thread/compaction/completed").length,
          0,
        );
        NodeAssert.equal(seen.filter((method) => method === "runtime/warning").length, 1);
        NodeAssert.equal(seen.filter((method) => method === "turn/completed").length, 1);
        yield* runtime.close;
      }),
    ),
  );

  it.effect("keeps unknown Pi events in a bounded ring without failing the session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const unknownEvents = Array.from({ length: 25 }, (_, index) => ({
          type: "future_event_kind",
          index,
        }));
        // Unknown frames first, then a real captured turn: the session must keep
        // working, and the newest frames must be the ones retained.
        const eventsPath = writeJsonl(unknownEvents);
        NodeFS.appendFileSync(eventsPath, NodeFS.readFileSync(NORMAL_COMPLETION, "utf8"));
        const harness = makeFakeCli({
          responses: {
            get_state: { success: true, data: getStateData() },
            prompt: { success: true },
          },
          events: eventsPath,
        });
        const runtime = yield* startRuntime(harness);
        yield* runtime.sendTurn({ text: "still works" });
        const events = yield* collectUntilTerminal(runtime);
        NodeAssert.equal(methods(events).filter((method) => method === "turn/completed").length, 1);

        const diagnostics = yield* runtime.unknownEvents;
        NodeAssert.equal(diagnostics.length, 20);
        NodeAssert.equal(diagnostics[0]?.type, "future_event_kind");
        NodeAssert.equal(diagnostics[0]?.frame.index, 5);
        NodeAssert.equal(diagnostics.at(-1)?.frame.index, 24);
        yield* runtime.close;
      }),
    ),
  );
});
