/**
 * PiAdapter — maps the Pi RPC session runtime onto T3's provider SPI.
 *
 * Everything Pi-specific about *turns* lives in `PiSessionRuntime`; this file
 * owns the translation into `ProviderRuntimeEvent`s, session bookkeeping, and
 * the attachment read needed to send images over RPC.
 *
 * @module provider/PiAdapter
 */
import {
  EventId,
  PI_DEFAULT_MODEL_SLUG,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  TurnId,
  isToolLifecycleItemType,
  type ChatAttachment,
  type PiSettings,
  type ProviderEvent,
  type ProviderRuntimeEvent,
  type ProviderTurnStartResult,
  type ThreadId,
  type ToolLifecycleItemType,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  decodePiResumeCursor,
  makePiSessionRuntime,
  type PiSessionRuntimeError,
  type PiSessionRuntimeOptions,
  type PiSessionRuntimeShape,
} from "./PiSessionRuntime.ts";

const PROVIDER = ProviderDriverKind.make("pi");

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly makeRuntime?: (
    options: PiSessionRuntimeOptions,
  ) => Effect.Effect<
    PiSessionRuntimeShape,
    PiSessionRuntimeError,
    ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
  >;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

interface PiAdapterSessionContext {
  readonly threadId: ThreadId;
  readonly scope: Scope.Closeable;
  readonly runtime: PiSessionRuntimeShape;
  readonly eventFiber: Fiber.Fiber<void, never>;
  stopped: boolean;
}

const isPiRuntimeError = (error: { readonly _tag: string }): error is PiSessionRuntimeError =>
  error._tag.startsWith("PiSession") || error._tag === "PiResumeCursorError";

const mapRuntimeError = (
  threadId: ThreadId,
  method: string,
  error: PiSessionRuntimeError,
): ProviderAdapterError => {
  switch (error._tag) {
    case "PiSessionSpawnError":
    case "PiSessionTransportError":
    case "PiSessionClosedError":
      return new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: error.message,
        cause: error,
      });
    case "PiSessionBusyError":
    case "PiResumeCursorError":
      return new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: method,
        issue: error.message,
        cause: error,
      });
    case "PiSessionRequestError":
      return new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: error.detail,
        cause: error,
      });
  }
};

/** Recovery errors must reach the caller as validation failures, not be folded
 * into a generic process error, so callers can tell "no session" from "broken". */
const toAdapterError = (
  threadId: ThreadId,
  method: string,
  cause: PiSessionRuntimeError | ProviderAdapterError,
): ProviderAdapterError =>
  isPiRuntimeError(cause) ? mapRuntimeError(threadId, method, cause) : cause;

const toolItemType = (candidate: unknown): ToolLifecycleItemType =>
  typeof candidate === "string" && isToolLifecycleItemType(candidate)
    ? candidate
    : "dynamic_tool_call";

/** Pi reads its thinking level from a per-model option; T3 stores it as the
 * `thinkingLevel` selection the model picker publishes. */
const thinkingLevelFromSelection = (
  options: ReadonlyArray<{ readonly id: string; readonly value: string | boolean }> | undefined,
): string | undefined => {
  const value = options?.find((option) => option.id === "thinkingLevel")?.value;
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
};

/** `pi-default` is a product slug, not a model id: it asks Pi to keep using the
 * model from its own configuration instead of T3 naming one. */
const requestedModelSlug = (
  selection: { readonly model: string } | undefined,
): string | undefined =>
  selection !== undefined && selection.model !== PI_DEFAULT_MODEL_SLUG
    ? selection.model
    : undefined;

/** `T3CODE_PI_LAUNCH_ARGS` mirrors the Codex convention: an env override wins
 * over the instance setting so tests and launch wrappers can inject argv. */
export const resolvePiLaunchArgs = (
  launchArgs: string,
  environment: NodeJS.ProcessEnv = process.env,
): ReadonlyArray<string> =>
  tokenizeCliArgs(environment.T3CODE_PI_LAUNCH_ARGS?.trim() || launchArgs.trim());

/**
 * Pure translation from the runtime's `ProviderEvent`s. `eventId`/`createdAt`
 * are reused from the runtime event so native logs and the UI agree on order.
 */
export const mapPiEventToRuntimeEvents = (
  event: ProviderEvent,
): ReadonlyArray<ProviderRuntimeEvent> => {
  const base = {
    eventId: event.id,
    provider: event.provider,
    ...(event.providerInstanceId ? { providerInstanceId: event.providerInstanceId } : {}),
    threadId: event.threadId,
    createdAt: event.createdAt,
    ...(event.turnId ? { turnId: event.turnId } : {}),
  } as const;

  switch (event.method) {
    case "session/started": {
      const payload = (event.payload ?? {}) as { readonly resume?: unknown };
      return [
        {
          ...base,
          type: "session.started",
          payload: payload.resume !== undefined ? { resume: payload.resume } : {},
        },
      ];
    }
    case "session/exited": {
      const payload = (event.payload ?? {}) as {
        readonly reason?: string;
        readonly recoverable?: boolean;
        readonly exitKind?: "graceful" | "error";
      };
      return [
        {
          ...base,
          type: "session.exited",
          payload: {
            ...(payload.reason ? { reason: payload.reason } : {}),
            ...(payload.recoverable !== undefined ? { recoverable: payload.recoverable } : {}),
            ...(payload.exitKind ? { exitKind: payload.exitKind } : {}),
          },
        },
      ];
    }
    case "session/model": {
      const payload = (event.payload ?? {}) as {
        readonly fromModel?: string;
        readonly toModel?: string;
        readonly reason?: string;
      };
      if (!payload.fromModel || !payload.toModel) return [];
      return [
        {
          ...base,
          type: "model.rerouted",
          payload: {
            fromModel: payload.fromModel,
            toModel: payload.toModel,
            reason: payload.reason ?? "pi-native-change",
          },
        },
      ];
    }
    case "session/name": {
      const payload = (event.payload ?? {}) as { readonly name?: string };
      if (!payload.name) return [];
      return [
        {
          ...base,
          type: "thread.metadata.updated",
          payload: { name: payload.name },
        },
      ];
    }
    case "turn/started": {
      const payload = (event.payload ?? {}) as {
        readonly model?: string;
        readonly effort?: string;
      };
      return [
        {
          ...base,
          type: "turn.started",
          payload: {
            ...(payload.model ? { model: payload.model } : {}),
            ...(payload.effort ? { effort: payload.effort } : {}),
          },
        },
      ];
    }
    case "turn/assistant/delta": {
      const delta = event.textDelta ?? "";
      if (delta.length === 0) return [];
      return [
        {
          ...base,
          type: "content.delta",
          payload: { streamKind: "assistant_text", delta },
        },
      ];
    }
    case "turn/item/started":
    case "turn/item/updated":
    case "turn/item/completed": {
      if (!event.itemId) return [];
      const payload = (event.payload ?? {}) as {
        readonly itemType?: unknown;
        readonly status?: "inProgress" | "completed" | "failed";
        readonly title?: string;
        readonly data?: unknown;
      };
      const lifecycle =
        event.method === "turn/item/started"
          ? ("item.started" as const)
          : event.method === "turn/item/updated"
            ? ("item.updated" as const)
            : ("item.completed" as const);
      return [
        {
          ...base,
          type: lifecycle,
          itemId: RuntimeItemId.make(event.itemId),
          payload: {
            itemType: toolItemType(payload.itemType),
            ...(payload.status ? { status: payload.status } : {}),
            ...(payload.title ? { title: payload.title } : {}),
            ...(payload.data !== undefined ? { data: payload.data } : {}),
          },
        },
      ];
    }
    case "turn/completed": {
      const payload = (event.payload ?? {}) as { readonly state?: string };
      const state =
        payload.state === "failed" ||
        payload.state === "interrupted" ||
        payload.state === "cancelled"
          ? payload.state
          : "completed";
      return [
        {
          ...base,
          type: "turn.completed",
          payload: { state },
        },
      ];
    }
    case "turn/aborted": {
      const payload = (event.payload ?? {}) as { readonly reason?: string };
      return [
        {
          ...base,
          type: "turn.aborted",
          payload: { reason: payload.reason ?? "Interrupted by user." },
        },
      ];
    }
    case "thread/compaction/completed":
      return [
        {
          ...base,
          type: "thread.state.changed",
          payload: { state: "compacted" },
        },
      ];
    case "runtime/warning": {
      const message = event.message ?? "Pi reported a warning.";
      return [
        {
          ...base,
          type: "runtime.warning",
          payload: { message },
        },
      ];
    }
    default:
      // `process/stderr`, `session/thinking`, and anything a newer Pi build
      // adds are diagnostics only: they must not become UI events.
      return [];
  }
};

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  piConfig: PiSettings,
  options?: PiAdapterLiveOptions,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const serverConfig = yield* ServerConfig;
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
  const nativeEventLogger =
    options?.nativeEventLogger ??
    (options?.nativeEventLogPath !== undefined
      ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
      : undefined);
  const managedNativeEventLogger =
    options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const sessions = new Map<ThreadId, PiAdapterSessionContext>();

  const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);
  const offerRuntimeEvents = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
    Effect.forEach(events, offerRuntimeEvent, { discard: true });
  // `randomUUIDv4` fails with a PlatformError; map it so adapter methods keep
  // their ProviderAdapterError channel.
  const makeEventId = (threadId: ThreadId) =>
    crypto.randomUUIDv4.pipe(
      Effect.map((id) => EventId.make(id)),
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId,
            detail: "Failed to generate an event id.",
            cause,
          }),
      ),
    );

  const writeNativeEvent = (event: ProviderEvent) =>
    nativeEventLogger ? nativeEventLogger.write(event, event.threadId) : Effect.void;

  const requireSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context || context.stopped) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "requireSession",
          issue: `No active Pi session for thread '${threadId}'.`,
        });
      }
      return context;
    });

  const stopSessionInternal = (context: PiAdapterSessionContext) =>
    Effect.gen(function* () {
      if (context.stopped) return;
      context.stopped = true;
      sessions.delete(context.threadId);
      yield* context.runtime.close.pipe(Effect.ignore);
      yield* Effect.ignore(Scope.close(context.scope, Exit.void));
      yield* Fiber.interrupt(context.eventFiber).pipe(Effect.ignore);
    });

  const startSession: PiAdapterShape["startSession"] = (input) =>
    Effect.scoped(
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }

        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped) {
          yield* stopSessionInternal(existing);
        }

        const resumeCursor = decodePiResumeCursor(input.resumeCursor);
        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const model = requestedModelSlug(modelSelection);
        const thinkingLevel = thinkingLevelFromSelection(modelSelection?.options);
        const launchArgs = resolvePiLaunchArgs(
          piConfig.launchArgs,
          options?.environment ?? process.env,
        );
        const runtimeInput: PiSessionRuntimeOptions = {
          threadId: input.threadId,
          providerInstanceId: boundInstanceId,
          binaryPath: piConfig.binaryPath,
          cwd: input.cwd ?? process.cwd(),
          environment: options?.environment ?? process.env,
          extendEnv: options?.environment === undefined,
          launchArgs,
          runtimeMode: input.runtimeMode,
          ...(piConfig.sessionDirPath.trim().length > 0
            ? { sessionDirPath: piConfig.sessionDirPath.trim() }
            : {}),
          ...(model ? { model } : {}),
          ...(thinkingLevel ? { thinkingLevel } : {}),
          ...(Option.isSome(resumeCursor) ? { resumeCursor: resumeCursor.value } : {}),
        };
        const sessionScope = yield* Scope.make("sequential");
        let sessionScopeTransferred = false;
        yield* Effect.addFinalizer(() =>
          sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
        );
        const createRuntime = options?.makeRuntime ?? makePiSessionRuntime;
        const runtime = yield* createRuntime(runtimeInput).pipe(
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) => toAdapterError(input.threadId, "startSession", cause)),
        );

        // Fork into the session scope: a fiber forked from `startSession` would be
        // interrupted when `startSession` returns, and every later runtime event
        // would be dropped.
        const eventFiber = yield* Stream.runForEach(runtime.events, (event) =>
          Effect.gen(function* () {
            yield* writeNativeEvent(event);
            yield* offerRuntimeEvents(mapPiEventToRuntimeEvents(event));
          }),
        ).pipe(Effect.forkIn(sessionScope));

        const context: PiAdapterSessionContext = {
          threadId: input.threadId,
          scope: sessionScope,
          runtime,
          eventFiber,
          stopped: false,
        };
        sessions.set(input.threadId, context);
        sessionScopeTransferred = true;

        const session = yield* runtime.getSession;
        if (input.runtimeMode !== "full-access") {
          // Pi's RPC surface has no approval channel, so T3 cannot enforce a
          // stricter mode here. Saying so beats a toggle that silently does nothing.
          yield* offerRuntimeEvent({
            type: "config.warning",
            eventId: yield* makeEventId(input.threadId),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            createdAt: session.updatedAt,
            payload: {
              summary: `Runtime mode '${input.runtimeMode}' is not enforced for Pi`,
              details:
                "Pi has no per-tool approval channel in this build, so tools run unattended. Configure Pi's own settings for stricter behaviour, or switch this thread to full access.",
            },
          });
        }
        return session;
      }),
    );

  const resolveImages = (attachments: ReadonlyArray<ChatAttachment> | undefined) =>
    Effect.forEach(
      (attachments ?? []).filter((attachment) => attachment.type === "image"),
      (attachment) =>
        Effect.gen(function* () {
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!attachmentPath) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          }
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "prompt",
                  detail: `Failed to read attachment '${attachment.id}': ${cause.message}`,
                  cause,
                }),
            ),
          );
          return {
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          };
        }),
    );

  const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      const images = yield* resolveImages(input.attachments);
      const modelSelection =
        input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
      const model = requestedModelSlug(modelSelection);
      const thinkingLevel = thinkingLevelFromSelection(modelSelection?.options);
      const started = yield* context.runtime.sendTurn({
        ...(input.input ? { text: input.input } : {}),
        ...(images.length > 0 ? { images } : {}),
        ...(model ? { model } : {}),
        ...(thinkingLevel ? { thinkingLevel } : {}),
      });
      return {
        threadId: input.threadId,
        turnId: started.turnId,
        ...(started.resumeCursor ? { resumeCursor: started.resumeCursor } : {}),
      } satisfies ProviderTurnStartResult;
    }).pipe(Effect.mapError((cause) => toAdapterError(input.threadId, "sendTurn", cause)));

  const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* context.runtime.interruptTurn;
    }).pipe(Effect.mapError((cause) => toAdapterError(threadId, "interruptTurn", cause)));

  const compactThread = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* context.runtime.compactThread;
    }).pipe(Effect.mapError((cause) => toAdapterError(threadId, "compactThread", cause)));

  const readThread: PiAdapterShape["readThread"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const messages = yield* context.runtime.readThreadMessages;
      return {
        threadId,
        turns: [
          {
            // Pi hands back no durable turn ids for history, so the transcript is
            // attributed to one synthetic turn keyed off the thread.
            id: TurnId.make(`pi-history:${threadId}`),
            items: messages,
          },
        ],
      };
    }).pipe(Effect.mapError((cause) => toAdapterError(threadId, "readThread", cause)));

  const rollbackThread: PiAdapterShape["rollbackThread"] = (_threadId) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail:
          "Pi sessions cannot be rewound by T3 in this build. Start a new thread to change history.",
      }),
    );

  const respondToRequest: PiAdapterShape["respondToRequest"] = (_threadId, requestId) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: `request/${requestId}`,
        detail: "Pi does not issue approval requests, so there is no pending request to answer.",
      }),
    );

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = (_threadId, requestId) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: `user-input/${requestId}`,
        detail: "Pi does not issue structured user-input requests in this build.",
      }),
    );

  const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) return;
      const session = yield* context.runtime.getSession.pipe(Effect.orElseSucceed(() => undefined));
      // Tell the UI before tearing the runtime down: after `close` the queue is
      // shut, so an exit event emitted then would never be drained.
      yield* offerRuntimeEvent({
        type: "session.exited",
        eventId: yield* makeEventId(threadId),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId,
        createdAt: session?.updatedAt ?? DateTime.formatIso(yield* DateTime.now),
        payload: { reason: "Stopped by user.", recoverable: true, exitKind: "graceful" },
      });
      yield* stopSessionInternal(context);
    });

  const listSessions: PiAdapterShape["listSessions"] = () =>
    Effect.forEach(
      Array.from(sessions.values()).filter((context) => !context.stopped),
      (context) => context.runtime.getSession,
      { concurrency: 1 },
    );

  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.succeed(Boolean(sessions.get(threadId) && !sessions.get(threadId)?.stopped));

  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(Array.from(sessions.values()), stopSessionInternal, {
      concurrency: 1,
      discard: true,
    }).pipe(Effect.asVoid);

  yield* Effect.acquireRelease(Effect.void, () =>
    stopAll().pipe(
      Effect.andThen(Queue.shutdown(runtimeEventQueue)),
      Effect.andThen(managedNativeEventLogger?.close() ?? Effect.void),
      Effect.ignore,
    ),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
      promptlessTurnContinuation: false,
      supportsConversationRollback: false,
    },
    startSession,
    sendTurn,
    compaction: { type: "native", start: (threadId) => compactThread(threadId) },
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  } satisfies PiAdapterShape;
});
