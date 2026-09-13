// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  EventId,
  PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  ThreadId,
  TurnId,
  type ProviderEvent,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { makePiAdapter, mapPiEventToRuntimeEvents } from "./PiAdapter.ts";
import type {
  PiResumeCursor,
  PiSendTurnInput,
  PiSessionRuntimeOptions,
  PiSessionRuntimeShape,
  PiTurnStart,
  PiUnknownEventRecord,
} from "./PiSessionRuntime.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const PROVIDER = ProviderDriverKind.make("pi");
const INSTANCE = ProviderInstanceId.make("pi");
const THREAD = ThreadId.make("thread-adapter-1");
const NOW = "2026-01-01T00:00:00.000Z";
const RESUME_CURSOR = {
  version: 1,
  sessionId: "session-1",
  sessionFile: "/tmp/session-1.jsonl",
} satisfies PiResumeCursor;

const piEvent = (event: Partial<ProviderEvent> & Pick<ProviderEvent, "method">): ProviderEvent => ({
  id: EventId.make("event-1"),
  kind: "notification",
  provider: PROVIDER,
  providerInstanceId: INSTANCE,
  threadId: THREAD,
  createdAt: NOW,
  ...event,
});

class FakePiRuntime implements PiSessionRuntimeShape {
  private readonly eventQueue = Effect.runSync(Queue.unbounded<ProviderEvent>());
  public readonly sendTurnInputs: Array<PiSendTurnInput> = [];
  public readonly counts = { interrupt: 0, compact: 0, close: 0 };

  public readonly options: PiSessionRuntimeOptions;

  constructor(options: PiSessionRuntimeOptions) {
    this.options = options;
  }

  // A getter, not a field: class-field initializers run before the parameter
  // property `options` is assigned.
  get getSession(): Effect.Effect<ProviderSession> {
    return Effect.succeed({
      provider: PROVIDER,
      providerInstanceId: INSTANCE,
      status: "ready" as const,
      runtimeMode: this.options.runtimeMode,
      cwd: this.options.cwd,
      threadId: this.options.threadId,
      model: "opencode-go/glm-5.3",
      resumeCursor: RESUME_CURSOR,
      createdAt: NOW,
      updatedAt: NOW,
    });
  }

  get events() {
    return Stream.fromQueue(this.eventQueue);
  }

  sendTurn(input: PiSendTurnInput): Effect.Effect<PiTurnStart> {
    this.sendTurnInputs.push(input);
    return Effect.succeed({ turnId: TurnId.make("turn-fake-1"), resumeCursor: RESUME_CURSOR });
  }

  get interruptTurn() {
    return Effect.sync(() => {
      this.counts.interrupt += 1;
    });
  }

  get compactThread() {
    return Effect.sync(() => {
      this.counts.compact += 1;
    });
  }

  readThreadMessages = Effect.succeed([
    { role: "user", content: [{ type: "text", text: "hi" }] },
  ] as ReadonlyArray<unknown>);

  unknownEvents = Effect.succeed([] as ReadonlyArray<PiUnknownEventRecord>);

  close = Effect.sync(() => {
    this.counts.close += 1;
  });

  closed = Effect.succeed(false);

  emit(event: ProviderEvent) {
    return Queue.offer(this.eventQueue, event).pipe(Effect.asVoid);
  }
}

const fakeRuntimeOptions = (
  runtimeMode: PiSessionRuntimeOptions["runtimeMode"] = "full-access",
): PiSessionRuntimeOptions => ({
  threadId: THREAD,
  providerInstanceId: INSTANCE,
  binaryPath: "pi",
  cwd: process.cwd(),
  environment: {},
  extendEnv: false,
  launchArgs: [],
  runtimeMode,
});

class PiAdapterService extends Context.Service<PiAdapterService, PiAdapterShape>()(
  "t3/provider/Layers/PiAdapter.test/PiAdapterService",
) {}

const sharedRuntime = new FakePiRuntime(fakeRuntimeOptions());

const adapterLayer = Layer.effect(
  PiAdapterService,
  makePiAdapter(decodePiSettings({ enabled: true }), {
    instanceId: INSTANCE,
    environment: { PATH: process.env.PATH ?? "" },
    makeRuntime: () => Effect.succeed(sharedRuntime),
  }),
).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(NodeServices.layer),
);

const THREADS = {
  start: ThreadId.make("thread-adapter-start"),
  warn: ThreadId.make("thread-adapter-warn"),
  turn: ThreadId.make("thread-adapter-turn"),
  compact: ThreadId.make("thread-adapter-compact"),
  stop: ThreadId.make("thread-adapter-stop"),
  attach: ThreadId.make("thread-adapter-attach"),
} as const;

const startSession = (
  adapter: PiAdapterShape,
  threadId: ThreadId,
  runtimeMode: "full-access" | "approval-required",
) =>
  adapter.startSession({
    threadId,
    providerInstanceId: INSTANCE,
    cwd: process.cwd(),
    runtimeMode,
    modelSelection: createModelSelection(INSTANCE, "opencode-go/glm-5.3", [
      { id: "thinkingLevel", value: "high" },
    ]),
  });

/**
 * The runtime event stream is queue-backed, so events emitted before this reads
 * are still delivered. The timeout is a hang guard: a missing event fails the
 * assertion rather than passing as "nothing arrived".
 */
const collectEvents = (adapter: PiAdapterShape, until: (event: ProviderRuntimeEvent) => boolean) =>
  adapter.streamEvents.pipe(
    Stream.takeUntil(until),
    Stream.runCollect,
    Effect.map((chunk) => Array.from(chunk)),
    Effect.timeoutOption("5 seconds"),
  );

const unwrap = (
  result: Option.Option<ReadonlyArray<ProviderRuntimeEvent>>,
): ReadonlyArray<ProviderRuntimeEvent> => {
  NodeAssert.ok(Option.isSome(result), "expected runtime events before the hang guard expired");
  return Option.getOrThrow(result);
};

it("maps Pi runtime events onto T3 runtime events", () => {
  const turnId = TurnId.make("turn-9");
  const delta = mapPiEventToRuntimeEvents(
    piEvent({ method: "turn/assistant/delta", turnId, textDelta: "hello" }),
  );
  NodeAssert.deepEqual(delta, [
    {
      eventId: EventId.make("event-1"),
      provider: PROVIDER,
      providerInstanceId: INSTANCE,
      threadId: THREAD,
      createdAt: NOW,
      turnId,
      type: "content.delta",
      payload: { streamKind: "assistant_text", delta: "hello" },
    },
  ]);

  // An empty delta is not a UI event.
  NodeAssert.deepEqual(
    mapPiEventToRuntimeEvents(piEvent({ method: "turn/assistant/delta", turnId, textDelta: "" })),
    [],
  );

  // A tool item keeps Pi's call id and maps its tool name onto a rendered type.
  const item = mapPiEventToRuntimeEvents(
    piEvent({
      method: "turn/item/started",
      turnId,
      itemId: ProviderItemId.make("call-1"),
      payload: { itemType: "file_change", title: "write", status: "inProgress" },
    }),
  );
  NodeAssert.equal(item[0]?.type, "item.started");
  NodeAssert.equal(item[0]?.itemId, ProviderItemId.make("call-1"));
  NodeAssert.deepEqual(item[0]?.payload, {
    itemType: "file_change",
    title: "write",
    status: "inProgress",
  });

  // An item type outside the rendered set degrades instead of disappearing.
  const unknownItem = mapPiEventToRuntimeEvents(
    piEvent({
      method: "turn/item/started",
      turnId,
      itemId: ProviderItemId.make("call-2"),
      payload: { itemType: "read" },
    }),
  );
  NodeAssert.equal(
    (unknownItem[0]?.payload as { itemType?: string }).itemType,
    "dynamic_tool_call",
  );

  // A native model change surfaces through model.rerouted.
  const rerouted = mapPiEventToRuntimeEvents(
    piEvent({
      method: "session/model",
      payload: { fromModel: "opencode-go/glm-5.3", toModel: "anthropic/claude-fable-5" },
    }),
  );
  NodeAssert.equal(rerouted[0]?.type, "model.rerouted");

  // Diagnostics never become UI events.
  NodeAssert.deepEqual(mapPiEventToRuntimeEvents(piEvent({ method: "process/stderr" })), []);
  NodeAssert.deepEqual(mapPiEventToRuntimeEvents(piEvent({ method: "session/thinking" })), []);
});

it.layer(adapterLayer)("PiAdapter session wiring", (it) => {
  it.effect("forwards the runtime's session start and stays silent on full access", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      const session = yield* startSession(adapter, THREADS.start, "full-access");
      NodeAssert.equal(session.provider, PROVIDER);
      NodeAssert.deepEqual(session.resumeCursor, RESUME_CURSOR);
      yield* sharedRuntime.emit(
        piEvent({
          method: "session/started",
          threadId: THREADS.start,
          payload: { resume: RESUME_CURSOR },
        }),
      );
      const events = unwrap(
        yield* collectEvents(adapter, (event) => event.type === "session.started"),
      );
      NodeAssert.equal(events[0]?.type, "session.started");
      NodeAssert.deepEqual(events[0]?.payload, { resume: RESUME_CURSOR });
      // Full access is the one mode Pi can honour, so it warns about nothing.
      NodeAssert.equal(events.filter((event) => event.type === "config.warning").length, 0);
    }),
  );

  it.effect("warns that a stricter runtime mode is not enforced", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      yield* startSession(adapter, THREADS.warn, "approval-required");
      const events = unwrap(
        yield* collectEvents(adapter, (event) => event.type === "config.warning"),
      );
      const warning = events.find((event) => event.type === "config.warning");
      NodeAssert.ok(warning, "a config.warning was emitted");
      NodeAssert.match((warning?.payload as { summary: string }).summary, /not enforced/u);
    }),
  );

  it.effect("sends the model selection and returns the runtime's turn", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      yield* startSession(adapter, THREADS.turn, "full-access");
      const started = yield* adapter.sendTurn({
        threadId: THREADS.turn,
        input: "do the thing",
        modelSelection: createModelSelection(INSTANCE, "anthropic/claude-fable-5", [
          { id: "thinkingLevel", value: "max" },
        ]),
      });
      NodeAssert.equal(started.turnId, TurnId.make("turn-fake-1"));
      NodeAssert.deepEqual(started.resumeCursor, RESUME_CURSOR);
      NodeAssert.deepEqual(sharedRuntime.sendTurnInputs.at(-1), {
        text: "do the thing",
        model: "anthropic/claude-fable-5",
        thinkingLevel: "max",
      });
    }),
  );

  it.effect("routes compaction through the runtime and reports unsupported requests", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      yield* startSession(adapter, THREADS.compact, "full-access");
      const compaction = adapter.compaction;
      NodeAssert.equal(compaction?.type, "native");
      const before = sharedRuntime.counts.compact;
      if (compaction?.type === "native") {
        yield* compaction.start(THREADS.compact);
      }
      NodeAssert.equal(sharedRuntime.counts.compact, before + 1);

      const rollback = yield* adapter.rollbackThread(THREADS.compact, 1).pipe(Effect.exit);
      NodeAssert.equal(rollback._tag, "Failure");
      const approval = yield* adapter
        .respondToRequest(THREADS.compact, "request-1" as never, "accept")
        .pipe(Effect.exit);
      NodeAssert.equal(approval._tag, "Failure");
      const userInput = yield* adapter
        .respondToUserInput(THREADS.compact, "request-2" as never, {})
        .pipe(Effect.exit);
      NodeAssert.equal(userInput._tag, "Failure");
      NodeAssert.equal(yield* adapter.hasSession(THREADS.compact), true);
      const history = yield* adapter.readThread(THREADS.compact);
      NodeAssert.equal(history.turns.length, 1);
    }),
  );

  it.effect("announces a graceful session exit when the session is stopped", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      yield* startSession(adapter, THREADS.stop, "full-access");
      const closesBefore = sharedRuntime.counts.close;
      yield* adapter.stopSession(THREADS.stop);
      const events = unwrap(
        yield* collectEvents(adapter, (event) => event.type === "session.exited"),
      );
      const exited = events.find((event) => event.type === "session.exited");
      NodeAssert.deepEqual(exited?.payload, {
        reason: "Stopped by user.",
        recoverable: true,
        exitKind: "graceful",
      });
      NodeAssert.equal(sharedRuntime.counts.close, closesBefore + 1);
      NodeAssert.equal(yield* adapter.hasSession(THREADS.stop), false);
    }),
  );
});

// Attachments are resolved against the server's own attachments directory, so
// this suite gets a temp base dir instead of the repo's real one.
const ATTACHMENT_BASE_DIR = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-attach-"));
const ATTACHMENTS_DIR = NodePath.join(ATTACHMENT_BASE_DIR, "userdata", "attachments");

const attachmentRuntime = new FakePiRuntime(fakeRuntimeOptions());

const attachmentAdapterLayer = Layer.effect(
  PiAdapterService,
  makePiAdapter(decodePiSettings({ enabled: true }), {
    instanceId: INSTANCE,
    environment: { PATH: process.env.PATH ?? "" },
    makeRuntime: () => Effect.succeed(attachmentRuntime),
  }),
).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), ATTACHMENT_BASE_DIR)),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(attachmentAdapterLayer)("PiAdapter attachments", (it) => {
  it.effect("reads an image attachment into the prompt's base64 images", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapterService;
      const attachmentId = "img-1a2b3c";
      const bytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
      NodeFS.mkdirSync(ATTACHMENTS_DIR, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(ATTACHMENTS_DIR, `${attachmentId}.png`), bytes);

      yield* startSession(adapter, THREADS.attach, "full-access");
      yield* adapter.sendTurn({
        threadId: THREADS.attach,
        input: "what is in this screenshot",
        attachments: [
          {
            type: "image",
            id: attachmentId,
            name: "pixel.png",
            mimeType: "image/png",
            sizeBytes: bytes.byteLength,
          },
        ],
      });

      NodeAssert.deepEqual(attachmentRuntime.sendTurnInputs.at(-1), {
        text: "what is in this screenshot",
        images: [{ data: bytes.toString("base64"), mimeType: "image/png" }],
      });
    }),
  );
});
