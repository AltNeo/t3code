/**
 * PiSessionRuntime — one `pi --mode rpc` process per T3 thread.
 *
 * Owns the process and the wire only: it emits `ProviderEvent`s named after
 * what happened (turn, item, session, compaction) and stamps the T3 `TurnId`
 * it mints per prompt. `PiAdapter` maps those onto `ProviderRuntimeEvent`s.
 *
 * Two Pi behaviours shape this file:
 *   - `agent_settled` is the terminal signal. `agent_end` may still be followed
 *     by a retry, a compaction retry, or a queued continuation, so completing a
 *     T3 turn at `agent_end` would report work that has not finished.
 *   - Resuming is `--session <file>` at spawn plus a `get_state` verification.
 *     `switch_session` reports success for a missing file and silently starts a
 *     blank session, which would lose a thread's history without an error.
 *
 * @module provider/PiSessionRuntime
 */
import {
  EventId,
  ProviderDriverKind,
  ProviderItemId,
  TurnId,
  type ProviderEvent,
  type ProviderSession,
  type ProviderInstanceId,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { expandHomePath } from "../../pathExpansion.ts";
import {
  makePiRpcConnection,
  type PiRpcConnectionShape,
  type PiRpcError,
} from "./PiRpcConnection.ts";
import {
  assistantTextFromMessage,
  decodePiAgentEnd,
  decodePiCompactionEnd,
  decodePiCompactionStart,
  decodePiExtensionUiRequest,
  decodePiMessageUpdate,
  decodePiMessages,
  decodePiSessionInfoChanged,
  decodePiSessionState,
  decodePiThinkingLevelChanged,
  decodePiToolExecutionEnd,
  decodePiToolExecutionStart,
  decodePiToolExecutionUpdate,
  decodePiTurnEnd,
  isPiDialogMethod,
  piDialogCancelledResponse,
  piModelSlug,
  splitPiModelSlug,
  type PiFrame,
  type PiResponse,
  type PiSessionState,
} from "./piRpcProtocol.ts";

/** Pi's own session identity, persisted by T3 and replayed on resume. */
export const PiResumeCursor = Schema.Struct({
  version: Schema.Literal(1),
  sessionId: Schema.NonEmptyString,
  sessionFile: Schema.NonEmptyString,
});
export type PiResumeCursor = typeof PiResumeCursor.Type;
export const decodePiResumeCursor = Schema.decodeUnknownOption(PiResumeCursor);

export class PiSessionSpawnError extends Schema.TaggedError<PiSessionSpawnError>()(
  "PiSessionSpawnError",
  {
    threadId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to start Pi for thread ${this.threadId}: ${this.detail}`;
  }
}

export class PiSessionTransportError extends Schema.TaggedError<PiSessionTransportError>()(
  "PiSessionTransportError",
  {
    threadId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi transport failure for thread ${this.threadId}: ${this.detail}`;
  }
}

export class PiSessionRequestError extends Schema.TaggedError<PiSessionRequestError>()(
  "PiSessionRequestError",
  {
    threadId: Schema.String,
    operation: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi rejected ${this.operation} for thread ${this.threadId}: ${this.detail}`;
  }
}

/** Resume could not be honoured. Failing closed is the whole point: Pi reports
 * success for a missing session file and creates a blank one instead. */
export class PiResumeCursorError extends Schema.TaggedError<PiResumeCursorError>()(
  "PiResumeCursorError",
  {
    threadId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Cannot resume the Pi session for thread ${this.threadId}: ${this.detail}`;
  }
}

export class PiSessionBusyError extends Schema.TaggedError<PiSessionBusyError>()(
  "PiSessionBusyError",
  {
    threadId: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Pi is already running a turn for thread ${this.threadId}: ${this.detail}`;
  }
}

export class PiSessionClosedError extends Schema.TaggedError<PiSessionClosedError>()(
  "PiSessionClosedError",
  {
    threadId: Schema.String,
  },
) {
  override get message(): string {
    return `The Pi session for thread ${this.threadId} is closed.`;
  }
}

export type PiSessionRuntimeError =
  | PiSessionSpawnError
  | PiSessionTransportError
  | PiSessionRequestError
  | PiResumeCursorError
  | PiSessionBusyError
  | PiSessionClosedError;

export interface PiImageAttachment {
  readonly data: string;
  readonly mimeType: string;
}

export interface PiSendTurnInput {
  readonly text?: string;
  readonly images?: ReadonlyArray<PiImageAttachment>;
  /** T3 slug (`provider/modelId`) plus the optional thinking level. */
  readonly model?: string;
  readonly thinkingLevel?: string;
}

export interface PiTurnStart {
  readonly turnId: TurnId;
  readonly resumeCursor?: PiResumeCursor;
}

export interface PiSessionRuntimeOptions {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly extendEnv: boolean;
  readonly launchArgs: ReadonlyArray<string>;
  readonly runtimeMode: RuntimeMode;
  readonly sessionDirPath?: string;
  readonly model?: string;
  readonly thinkingLevel?: string;
  readonly resumeCursor?: PiResumeCursor;
}

export interface PiSessionRuntimeShape {
  readonly events: Stream.Stream<ProviderEvent>;
  readonly getSession: Effect.Effect<ProviderSession>;
  readonly sendTurn: (input: PiSendTurnInput) => Effect.Effect<PiTurnStart, PiSessionRuntimeError>;
  readonly interruptTurn: Effect.Effect<void, PiSessionRuntimeError>;
  /** Manual compaction; resolves once Pi reports the compaction finished. */
  readonly compactThread: Effect.Effect<void, PiSessionRuntimeError>;
  readonly readThreadMessages: Effect.Effect<ReadonlyArray<unknown>, PiSessionRuntimeError>;
  /** Unrecognised frames Pi sent, oldest first and bounded: a newer Pi build
   * adding an event type stays diagnosable instead of invisible. */
  readonly unknownEvents: Effect.Effect<ReadonlyArray<PiUnknownEventRecord>>;
  readonly close: Effect.Effect<void>;
  readonly closed: Effect.Effect<boolean>;
}

/** One frame this build does not model, kept verbatim for diagnostics. */
export interface PiUnknownEventRecord {
  readonly type: string;
  readonly frame: Readonly<Record<string, unknown>>;
}

/** What Pi reports about one compaction. The T3 verdict comes from here rather
 * than the `compact` command response, which can acknowledge a compaction that
 * then aborts or fails. */
interface PiCompactionOutcome {
  readonly aborted: boolean;
  readonly errorMessage?: string;
}

/** Item payloads this runtime hands to the adapter, which maps them onto
 * `ItemLifecyclePayload`. The Pi tool name always travels in `title`. */
export interface PiItemPayload {
  readonly itemType: string;
  readonly status?: "inProgress" | "completed" | "failed";
  readonly title?: string;
  readonly data?: unknown;
}

export interface PiItemEventPayload {
  readonly turnId: TurnId;
  readonly itemId: string;
  readonly item: PiItemPayload;
}

/** Pi tool name → the closed set of item types ingestion renders. Anything
 * outside the set is dropped by ingestion, so unknown tools degrade to a
 * generic tool row rather than disappearing. */
export const piItemTypeForTool = (toolName: string): string => {
  switch (toolName) {
    case "bash":
    case "shell":
      return "command_execution";
    case "write":
    case "edit":
    case "patch":
    case "apply_patch":
      return "file_change";
    case "webfetch":
    case "websearch":
    case "web_search":
      return "web_search";
    case "task":
    case "subagent":
    case "subagent_supervisor":
      return "collab_agent_tool_call";
    default:
      return "dynamic_tool_call";
  }
};

interface ActiveTurn {
  readonly turnId: TurnId;
  startedEmitted: boolean;
  abortRequested: boolean;
  settled: boolean;
  sawText: boolean;
  readonly toolData: Map<string, unknown>;
}

const PI_READY_TIMEOUT = "30 seconds" as const;
const PI_ABORT_TIMEOUT = "2 minutes" as const;
const PI_COMPACT_TIMEOUT = "9 minutes" as const;
const PI_UNKNOWN_EVENT_LIMIT = 20;
const PLACEHOLDER_MODEL_ID = "unknown";
const PROVIDER = ProviderDriverKind.make("pi");

export const makePiSessionRuntime = (
  options: PiSessionRuntimeOptions,
): Effect.Effect<
  PiSessionRuntimeShape,
  PiSessionRuntimeError,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const runtimeScope = yield* Scope.Scope;
    const events = yield* Queue.unbounded<ProviderEvent>();
    const activeTurnRef = yield* Ref.make<ActiveTurn | undefined>(undefined);
    const closedRef = yield* Ref.make(false);
    const manualCompactionRef = yield* Ref.make<
      Option.Option<Deferred.Deferred<PiCompactionOutcome>>
    >(Option.none());
    const unknownEventsRef = yield* Ref.make<ReadonlyArray<PiUnknownEventRecord>>([]);
    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    // `randomUUIDv4` fails with a PlatformError; the rest of this module only
    // speaks PiSessionRuntimeError, so normalize it here.
    const randomUUID = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new PiSessionTransportError({
            threadId: options.threadId,
            detail: "Failed to generate an identifier.",
            cause,
          }),
      ),
    );

    const launchArgs = options.launchArgs;
    const spawnArgs: Array<string> = ["--mode", "rpc"];
    if (options.sessionDirPath !== undefined && options.sessionDirPath.trim().length > 0) {
      spawnArgs.push("--session-dir", expandHomePath(options.sessionDirPath.trim()));
    }
    if (options.resumeCursor !== undefined) {
      // Resume by absolute session file: Pi's id prefixes are ambiguous.
      spawnArgs.push("--session", options.resumeCursor.sessionFile);
    } else if (options.model !== undefined) {
      spawnArgs.push("--model", options.model);
      if (options.thinkingLevel !== undefined && options.thinkingLevel.trim().length > 0) {
        spawnArgs.push("--thinking", options.thinkingLevel.trim());
      }
    }
    spawnArgs.push(...launchArgs);

    const connection: PiRpcConnectionShape = yield* makePiRpcConnection({
      binaryPath: options.binaryPath,
      args: spawnArgs,
      cwd: options.cwd,
      env: options.environment,
      extendEnv: options.extendEnv,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new PiSessionSpawnError({
            threadId: options.threadId,
            detail: cause.message,
            cause,
          }),
      ),
    );

    const createdAt = yield* nowIso;
    const sessionRef = yield* Ref.make<ProviderSession>({
      provider: PROVIDER,
      providerInstanceId: options.providerInstanceId,
      status: "connecting",
      runtimeMode: options.runtimeMode,
      cwd: options.cwd,
      ...(options.model !== undefined ? { model: options.model } : {}),
      threadId: options.threadId,
      ...(options.resumeCursor !== undefined ? { resumeCursor: options.resumeCursor } : {}),
      createdAt,
      updatedAt: createdAt,
    });

    const updateSession = (patch: Partial<ProviderSession>) =>
      Effect.gen(function* () {
        const updatedAt = yield* nowIso;
        return yield* Ref.updateAndGet(sessionRef, (current) => ({
          ...current,
          ...patch,
          updatedAt,
        }));
      });

    const emitEvent = (event: Omit<ProviderEvent, "id" | "provider" | "createdAt">) =>
      Effect.gen(function* () {
        const id = yield* randomUUID;
        yield* Queue.offer(events, {
          id: EventId.make(id),
          provider: PROVIDER,
          providerInstanceId: options.providerInstanceId,
          createdAt: yield* nowIso,
          ...event,
        });
      });

    const emitWarning = (method: string, message: string, payload?: unknown) =>
      emitEvent({
        kind: "notification",
        threadId: options.threadId,
        method,
        message,
        ...(payload !== undefined ? { payload } : {}),
      });

    const mapTransportError = (error: PiRpcError): PiSessionRuntimeError => {
      switch (error._tag) {
        case "PiRpcSpawnError":
        case "PiRpcWriteError":
        case "PiRpcProcessExitedError":
          return new PiSessionTransportError({
            threadId: options.threadId,
            detail: error.message,
            cause: error,
          });
        case "PiRpcRequestError":
          return new PiSessionRequestError({
            threadId: options.threadId,
            operation: error.command,
            detail: error.detail,
            cause: error,
          });
      }
    };

    const readResponse = (
      response: PiResponse,
      operation: string,
    ): Effect.Effect<unknown, PiSessionRequestError> => {
      if (!response.success) {
        return Effect.fail(
          new PiSessionRequestError({
            threadId: options.threadId,
            operation,
            detail: response.error ?? "Pi reported an unspecified failure.",
          }),
        );
      }
      return Effect.succeed(response.data);
    };

    const request = (
      frame: Readonly<Record<string, unknown>>,
      operation: string,
      timeout?: Duration.Input,
    ): Effect.Effect<unknown, PiSessionRuntimeError> => {
      const answered = connection.request(frame).pipe(
        Effect.mapError(mapTransportError),
        Effect.flatMap((response) => readResponse(response, operation)),
      );
      if (timeout === undefined) return answered;
      return answered.pipe(
        Effect.timeout(timeout),
        Effect.catchTag("TimeoutError", () =>
          Effect.fail(
            new PiSessionRequestError({
              threadId: options.threadId,
              operation,
              detail: `Pi did not answer '${operation}' within ${timeout}.`,
            }),
          ),
        ),
      );
    };

    const requestState = (
      operation: string,
    ): Effect.Effect<PiSessionState, PiSessionRuntimeError> =>
      request({ type: "get_state" }, operation).pipe(
        Effect.flatMap((data) => {
          const decoded = decodePiSessionState(data);
          return decoded._tag === "Some"
            ? Effect.succeed(decoded.value)
            : Effect.fail(
                new PiSessionRequestError({
                  threadId: options.threadId,
                  operation,
                  detail: "Pi returned an unreadable get_state payload.",
                }),
              );
        }),
      );

    /** T3 slugs seen in `get_state.model`, used to publish Pi's live model. */
    const observedModelSlug = (state: PiSessionState): string | undefined =>
      state.model !== undefined && state.model.id !== PLACEHOLDER_MODEL_ID
        ? piModelSlug(state.model)
        : undefined;

    const currentResumeCursor = (state: PiSessionState): PiResumeCursor | undefined =>
      state.sessionId !== undefined &&
      state.sessionFile !== undefined &&
      state.sessionId.length > 0 &&
      state.sessionFile.length > 0
        ? { version: 1, sessionId: state.sessionId, sessionFile: state.sessionFile }
        : undefined;

    // The model T3 last asked for. Comparing against this — rather than against
    // Pi's current model — is what stops a Pi-native `/model` change from being
    // silently reverted by the next turn's stale thread selection.
    const lastRequestedModelRef = yield* Ref.make<string | undefined>(options.model);
    const lastRequestedThinkingRef = yield* Ref.make<string | undefined>(options.thinkingLevel);
    const observedModelRef = yield* Ref.make<string | undefined>(undefined);

    const publishObservedModel = (state: PiSessionState) =>
      Effect.gen(function* () {
        const slug = observedModelSlug(state);
        if (slug === undefined) return undefined;
        const previous = yield* Ref.getAndSet(observedModelRef, slug);
        yield* updateSession({ model: slug });
        if (previous !== undefined && previous !== slug) {
          yield* emitEvent({
            kind: "notification",
            threadId: options.threadId,
            method: "session/model",
            payload: { fromModel: previous, toModel: slug, reason: "pi-native-change" },
          });
        }
        return slug;
      });

    const applyModelSelection = (model: string | undefined, thinkingLevel: string | undefined) =>
      Effect.gen(function* () {
        const requestedModel = yield* Ref.get(lastRequestedModelRef);
        if (model !== undefined && model !== requestedModel) {
          const target = splitPiModelSlug(model);
          if (target === undefined) {
            return yield* new PiSessionRequestError({
              threadId: options.threadId,
              operation: "set_model",
              detail: `'${model}' is not a provider-qualified Pi model id.`,
            });
          }
          yield* request(
            { type: "set_model", provider: target.provider, modelId: target.modelId },
            "set_model",
            PI_READY_TIMEOUT,
          );
          yield* Ref.set(lastRequestedModelRef, model);
        }
        const requestedThinking = yield* Ref.get(lastRequestedThinkingRef);
        if (
          thinkingLevel !== undefined &&
          thinkingLevel.trim().length > 0 &&
          thinkingLevel !== requestedThinking
        ) {
          yield* request(
            { type: "set_thinking_level", level: thinkingLevel.trim() },
            "set_thinking_level",
            PI_READY_TIMEOUT,
          );
          yield* Ref.set(lastRequestedThinkingRef, thinkingLevel.trim());
        }
        return undefined;
      });

    const ensureTurnStarted = (turn: ActiveTurn, state?: PiSessionState) =>
      Effect.gen(function* () {
        if (turn.startedEmitted) return;
        turn.startedEmitted = true;
        const model =
          state !== undefined ? observedModelSlug(state) : yield* Ref.get(observedModelRef);
        const thinking =
          state !== undefined ? state.thinkingLevel : yield* Ref.get(lastRequestedThinkingRef);
        yield* emitEvent({
          kind: "notification",
          threadId: options.threadId,
          method: "turn/started",
          turnId: turn.turnId,
          payload: {
            ...(model !== undefined ? { model } : {}),
            ...(thinking !== undefined ? { effort: thinking } : {}),
          },
        });
      });

    const settleTurn = (reason?: string) =>
      Effect.gen(function* () {
        const turn = yield* Ref.get(activeTurnRef);
        if (turn === undefined || turn.settled) return;
        turn.settled = true;
        yield* Ref.set(activeTurnRef, undefined);
        // Read the live model back: Pi can change it natively (a `/model`
        // command, a provider fallback), and success booleans are not proof.
        const state = yield* requestState("get_state").pipe(Effect.orElseSucceed(() => undefined));
        if (state !== undefined) {
          yield* publishObservedModel(state);
        }
        yield* updateSession({
          status: "ready",
          activeTurnId: undefined,
          lastError: undefined,
        });
        const aborted = turn.abortRequested;
        yield* emitEvent({
          kind: "notification",
          threadId: options.threadId,
          method: aborted ? "turn/aborted" : "turn/completed",
          turnId: turn.turnId,
          ...(aborted
            ? { payload: { reason: reason ?? "Interrupted by user." } }
            : { payload: { state: "completed" } }),
        });
      });

    const handleFrame = (frame: PiFrame) =>
      Effect.gen(function* () {
        const activeTurn = yield* Ref.get(activeTurnRef);
        switch (frame.type) {
          case "agent_start": {
            if (activeTurn !== undefined) {
              yield* ensureTurnStarted(activeTurn);
            }
            return;
          }
          case "message_update": {
            const decoded = decodePiMessageUpdate(frame);
            if (Option.isNone(decoded) || activeTurn === undefined) return;
            const assistantEvent = decoded.value.assistantMessageEvent;
            if (assistantEvent.type === "text_delta" && typeof assistantEvent.delta === "string") {
              yield* ensureTurnStarted(activeTurn);
              activeTurn.sawText = true;
              yield* emitEvent({
                kind: "notification",
                threadId: options.threadId,
                method: "turn/assistant/delta",
                turnId: activeTurn.turnId,
                textDelta: assistantEvent.delta,
              });
            }
            // `thinking_*` deltas are log-only: T3 has no reasoning surface, and
            // repainting on each one would burn frames for nothing.
            return;
          }
          case "tool_execution_start": {
            const decoded = decodePiToolExecutionStart(frame);
            if (Option.isNone(decoded) || activeTurn === undefined) return;
            const tool = decoded.value;
            yield* ensureTurnStarted(activeTurn);
            const data = tool.args === undefined ? undefined : { args: tool.args };
            if (data !== undefined) {
              activeTurn.toolData.set(tool.toolCallId, data);
            }
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/item/started",
              turnId: activeTurn.turnId,
              itemId: ProviderItemId.make(tool.toolCallId),
              payload: {
                itemType: piItemTypeForTool(tool.toolName),
                status: "inProgress",
                title: tool.toolName,
                ...(data !== undefined ? { data } : {}),
              },
            });
            return;
          }
          case "tool_execution_update": {
            const decoded = decodePiToolExecutionUpdate(frame);
            if (Option.isNone(decoded) || activeTurn === undefined) return;
            const tool = decoded.value;
            yield* ensureTurnStarted(activeTurn);
            const previous = activeTurn.toolData.get(tool.toolCallId);
            // `partialResult` is the accumulated result so far, not a new
            // fragment, so it replaces the previous value instead of appending.
            const data = {
              ...(typeof previous === "object" && previous !== null ? previous : {}),
              ...(tool.args !== undefined ? { args: tool.args } : {}),
              ...(tool.partialResult !== undefined ? { partialResult: tool.partialResult } : {}),
            };
            activeTurn.toolData.set(tool.toolCallId, data);
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/item/updated",
              turnId: activeTurn.turnId,
              itemId: ProviderItemId.make(tool.toolCallId),
              payload: {
                itemType: piItemTypeForTool(tool.toolName),
                status: "inProgress",
                title: tool.toolName,
                data,
              },
            });
            return;
          }
          case "tool_execution_end": {
            const decoded = decodePiToolExecutionEnd(frame);
            if (Option.isNone(decoded) || activeTurn === undefined) return;
            const tool = decoded.value;
            yield* ensureTurnStarted(activeTurn);
            const previous = activeTurn.toolData.get(tool.toolCallId);
            const isError = tool.isError === true;
            const data = {
              ...(typeof previous === "object" && previous !== null ? previous : {}),
              ...(tool.result !== undefined ? { result: tool.result } : {}),
              isError,
            };
            activeTurn.toolData.delete(tool.toolCallId);
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/item/completed",
              turnId: activeTurn.turnId,
              itemId: ProviderItemId.make(tool.toolCallId),
              payload: {
                itemType: piItemTypeForTool(tool.toolName),
                status: isError ? "failed" : "completed",
                title: tool.toolName,
                data,
              },
            });
            return;
          }
          case "turn_end": {
            const decoded = decodePiTurnEnd(frame);
            if (Option.isNone(decoded) || activeTurn === undefined) return;
            if (activeTurn.sawText) return;
            const text = assistantTextFromMessage(decoded.value.message);
            if (text === undefined) return;
            yield* ensureTurnStarted(activeTurn);
            activeTurn.sawText = true;
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/assistant/delta",
              turnId: activeTurn.turnId,
              textDelta: text,
            });
            return;
          }
          case "agent_end": {
            const decoded = decodePiAgentEnd(frame);
            // `willRetry` means Pi will run again inside the same prompt, so the
            // T3 turn stays open until `agent_settled` arrives.
            if (Option.isSome(decoded) && decoded.value.willRetry === true) {
              return;
            }
            return;
          }
          case "agent_settled": {
            yield* settleTurn();
            return;
          }
          case "compaction_start": {
            const decoded = decodePiCompactionStart(frame);
            if (Option.isNone(decoded)) return;
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/compaction/started",
              payload: { reason: decoded.value.reason ?? "unknown" },
            });
            return;
          }
          case "compaction_end": {
            const decoded = decodePiCompactionEnd(frame);
            if (Option.isNone(decoded)) return;
            const result = decoded.value;
            const succeeded = result.aborted !== true && result.errorMessage === undefined;
            if (!succeeded) {
              yield* emitWarning(
                "runtime/warning",
                result.errorMessage ?? "Pi context compaction did not complete.",
              );
            }
            // A manual compaction is settled through its outcome deferred, so
            // `compactThread` decides from this event instead of the command
            // response. Auto-compaction reports itself here.
            const pending = yield* Ref.getAndSet(manualCompactionRef, Option.none());
            if (Option.isSome(pending)) {
              yield* Deferred.succeed(pending.value, {
                aborted: result.aborted === true,
                ...(result.errorMessage === undefined ? {} : { errorMessage: result.errorMessage }),
              });
              return;
            }
            if (!succeeded) return;
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "thread/compaction/completed",
            });
            return;
          }
          case "extension_ui_request": {
            const decoded = decodePiExtensionUiRequest(frame);
            if (Option.isNone(decoded)) return;
            const request = decoded.value;
            if (!isPiDialogMethod(request.method)) {
              // notify/setStatus/setWidget/... are fire-and-forget.
              return;
            }
            // Milestone 1 has no dialog bridge. Answering keeps an extension
            // from blocking the agent forever, and the warning tells the user
            // the prompt was skipped rather than answered.
            yield* connection.notify(piDialogCancelledResponse(request.id)).pipe(Effect.ignore);
            yield* emitWarning(
              "runtime/warning",
              `Pi extension requested '${request.method}' input, which this build cannot answer. The prompt was declined.`,
              { method: request.method, title: request.title ?? null },
            );
            return;
          }
          case "session_info_changed": {
            const decoded = decodePiSessionInfoChanged(frame);
            if (Option.isNone(decoded)) return;
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "session/name",
              payload: { name: decoded.value.name },
            });
            return;
          }
          case "thinking_level_changed": {
            const decoded = decodePiThinkingLevelChanged(frame);
            if (Option.isNone(decoded)) return;
            yield* Ref.set(lastRequestedThinkingRef, decoded.value.level);
            yield* emitWarning(
              "session/thinking",
              `Pi thinking level is now ${decoded.value.level}.`,
            );
            return;
          }
          case "turn_start":
          case "message_start":
          case "message_end":
            // Pi's inner model turns and per-message boundaries are not T3
            // turns, and the user message is already in T3, so these are
            // consumed and dropped on purpose. They stay out of the
            // diagnostics ring so it only holds frames that signal drift.
            return;
          default: {
            // Unknown/future event types are ignored functionally: Pi may add
            // events, and a session must not fail because of one. The frames are
            // still retained so a protocol drift can be diagnosed.
            yield* Ref.update(unknownEventsRef, (current) =>
              [...current, { type: frame.type, frame }].slice(-PI_UNKNOWN_EVENT_LIMIT),
            );
            return;
          }
        }
      });

    yield* Stream.runForEach(connection.frames, handleFrame).pipe(Effect.forkIn(runtimeScope));

    yield* Stream.runForEach(connection.stderrLines, (line) =>
      line.trim().length === 0
        ? Effect.void
        : emitEvent({
            kind: "notification",
            threadId: options.threadId,
            method: "process/stderr",
            message: line,
          }),
    ).pipe(Effect.forkIn(runtimeScope));

    // Unexpected exit: settle the in-flight turn and tell the server the
    // session is gone, so a crashed turn does not stay "running" forever.
    yield* connection.exited.pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const alreadyClosed = yield* Ref.get(closedRef);
          if (alreadyClosed) return;
          const turn = yield* Ref.get(activeTurnRef);
          if (turn !== undefined && !turn.settled) {
            turn.settled = true;
            yield* Ref.set(activeTurnRef, undefined);
            yield* emitEvent({
              kind: "notification",
              threadId: options.threadId,
              method: "turn/aborted",
              turnId: turn.turnId,
              payload: { reason: `Pi exited with code ${exit.code}.` },
            });
          }
          yield* updateSession({ status: "error", activeTurnId: undefined });
          yield* emitEvent({
            kind: "session",
            threadId: options.threadId,
            method: "session/exited",
            message: `Pi exited with code ${exit.code}.`,
            payload: {
              reason: `Pi exited with code ${exit.code}.`,
              recoverable: false,
              exitKind: exit.code === 0 ? "graceful" : "error",
            },
          });
        }),
      ),
      Effect.forkIn(runtimeScope),
    );

    const startState = yield* requestState("get_state").pipe(
      Effect.timeout(PI_READY_TIMEOUT),
      Effect.catchTag("TimeoutError", () =>
        Effect.fail(
          new PiSessionSpawnError({
            threadId: options.threadId,
            detail: "Pi did not answer get_state within 30 seconds of starting.",
          }),
        ),
      ),
      Effect.onError(() => connection.close.pipe(Effect.ignore)),
    );

    if (options.resumeCursor !== undefined) {
      if (startState.sessionId !== options.resumeCursor.sessionId) {
        yield* connection.close.pipe(Effect.ignore);
        return yield* new PiResumeCursorError({
          threadId: options.threadId,
          detail: `Pi resumed session '${startState.sessionId ?? "none"}' instead of '${options.resumeCursor.sessionId}'. The stored session file is missing, moved, or belongs to another working directory.`,
        });
      }
      yield* Ref.set(sessionRef, {
        ...(yield* Ref.get(sessionRef)),
        resumeCursor: options.resumeCursor,
      });
    }

    const resumeCursor = currentResumeCursor(startState);
    if (resumeCursor === undefined) {
      // Without a session file Pi is ephemeral, and T3 could never recover the
      // thread. That is a configuration error worth failing loudly on.
      yield* connection.close.pipe(Effect.ignore);
      return yield* new PiSessionSpawnError({
        threadId: options.threadId,
        detail:
          "Pi started without a session file. Check that session persistence is enabled for this Pi installation.",
      });
    }

    yield* Ref.set(observedModelRef, observedModelSlug(startState));
    const initialModel = observedModelSlug(startState);
    if (initialModel !== undefined && options.model === undefined) {
      yield* Ref.set(lastRequestedModelRef, initialModel);
    }
    if (startState.thinkingLevel !== undefined && options.thinkingLevel === undefined) {
      yield* Ref.set(lastRequestedThinkingRef, startState.thinkingLevel);
    }
    yield* Ref.set(sessionRef, {
      ...(yield* Ref.get(sessionRef)),
      status: "ready",
      resumeCursor,
      ...(initialModel !== undefined ? { model: initialModel } : {}),
    });
    yield* emitEvent({
      kind: "session",
      threadId: options.threadId,
      method: "session/started",
      payload: { resume: resumeCursor },
    });

    const isClosed = Ref.get(closedRef);

    const sendTurn = (input: PiSendTurnInput) =>
      Effect.gen(function* () {
        if (yield* isClosed) {
          return yield* new PiSessionClosedError({ threadId: options.threadId });
        }
        const existing = yield* Ref.get(activeTurnRef);
        if (existing !== undefined && !existing.settled) {
          return yield* new PiSessionBusyError({
            threadId: options.threadId,
            detail:
              "A prompt is already in flight. Pi only accepts a queued message with an explicit streamingBehavior.",
          });
        }
        const text = input.text?.trim() ?? "";
        const images = input.images ?? [];
        if (text.length === 0 && images.length === 0) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "prompt",
            detail: "Turn requires non-empty text or attachments.",
          });
        }

        yield* applyModelSelection(input.model, input.thinkingLevel);

        const turnId = TurnId.make(yield* randomUUID);
        const turn: ActiveTurn = {
          turnId,
          startedEmitted: false,
          abortRequested: false,
          settled: false,
          sawText: false,
          toolData: new Map(),
        };
        yield* Ref.set(activeTurnRef, turn);
        yield* updateSession({ status: "running", activeTurnId: turnId, lastError: undefined });

        const response = yield* connection
          .request({
            type: "prompt",
            message: text.length > 0 ? text : "(see attached image)",
            ...(images.length > 0
              ? {
                  images: images.map((image) => ({
                    type: "image",
                    data: image.data,
                    mimeType: image.mimeType,
                  })),
                }
              : {}),
          })
          .pipe(Effect.mapError(mapTransportError));

        if (!response.success) {
          yield* Ref.set(activeTurnRef, undefined);
          yield* updateSession({ status: "ready", activeTurnId: undefined });
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "prompt",
            detail: response.error ?? "Pi rejected the prompt.",
          });
        }

        const state = yield* requestState("get_state").pipe(Effect.orElseSucceed(() => undefined));
        if (state !== undefined) {
          yield* ensureTurnStarted(turn, state);
        }
        const session = yield* Ref.get(sessionRef);
        const cursor = decodePiResumeCursor(session.resumeCursor);
        return {
          turnId,
          ...(Option.isSome(cursor) ? { resumeCursor: cursor.value } : {}),
        } satisfies PiTurnStart;
      });

    const interruptTurn = Effect.gen(function* () {
      if (yield* isClosed) {
        return yield* new PiSessionClosedError({ threadId: options.threadId });
      }
      const turn = yield* Ref.get(activeTurnRef);
      if (turn === undefined || turn.settled) {
        // Pi accepts an idle abort, but there is nothing here to interrupt.
        return;
      }
      turn.abortRequested = true;
      const result = yield* connection
        .request({ type: "abort" })
        .pipe(
          Effect.mapError(mapTransportError),
          Effect.timeoutOption(PI_ABORT_TIMEOUT),
          Effect.result,
        );
      // Pi answers `abort` after the run settles, but a failure or timeout must
      // still leave the UI with a terminal event instead of a running turn.
      if (result._tag === "Failure") {
        yield* settleTurn("Interrupted by user after an abort error.");
        return yield* result.failure;
      }
      if (Option.isNone(result.success)) {
        yield* settleTurn("Interrupted by user after Pi did not acknowledge the abort.");
        return yield* new PiSessionRequestError({
          threadId: options.threadId,
          operation: "abort",
          detail: "Pi did not acknowledge the abort in time.",
        });
      }
      yield* settleTurn();
    });

    const compactThread = Effect.gen(function* () {
      if (yield* isClosed) {
        return yield* new PiSessionClosedError({ threadId: options.threadId });
      }
      const turn = yield* Ref.get(activeTurnRef);
      if (turn !== undefined && !turn.settled) {
        return yield* new PiSessionBusyError({
          threadId: options.threadId,
          detail: "Cannot compact while a turn is running.",
        });
      }
      // Installed before the command is sent: Pi reports `compaction_end`
      // ahead of the `compact` response, so a deferred created afterwards
      // would miss the outcome entirely.
      const outcome = yield* Deferred.make<PiCompactionOutcome>();
      yield* Ref.set(manualCompactionRef, Option.some(outcome));
      return yield* Effect.gen(function* () {
        const result = yield* connection
          .request({ type: "compact" })
          .pipe(
            Effect.mapError(mapTransportError),
            Effect.timeoutOption(PI_COMPACT_TIMEOUT),
            Effect.result,
          );
        if (result._tag === "Failure") {
          return yield* result.failure;
        }
        if (Option.isNone(result.success)) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "compact",
            detail: "Pi did not finish compacting in time.",
          });
        }
        const response = result.success.value;
        if (!response.success) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "compact",
            detail: response.error ?? "Pi reported that compaction failed.",
          });
        }
        // The response only proves the command was accepted; `compaction_end`
        // is what says the compaction actually happened. Pi answers `compact`
        // successfully even when it aborts or fails afterwards.
        const settled = yield* Deferred.await(outcome).pipe(
          Effect.timeoutOption(PI_COMPACT_TIMEOUT),
        );
        if (Option.isNone(settled)) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "compact",
            detail: "Pi accepted the compaction but never reported a result.",
          });
        }
        const reported = settled.value;
        if (reported.aborted || reported.errorMessage !== undefined) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: "compact",
            detail: reported.errorMessage ?? "Pi aborted the compaction.",
          });
        }
        yield* emitEvent({
          kind: "notification",
          threadId: options.threadId,
          method: "thread/compaction/completed",
        });
      }).pipe(Effect.ensuring(Ref.set(manualCompactionRef, Option.none())));
    });

    const readThreadMessages = request({ type: "get_messages" }, "get_messages").pipe(
      Effect.flatMap((data) => {
        const decoded = decodePiMessages(data);
        return Effect.succeed(Option.isSome(decoded) ? decoded.value.messages : []);
      }),
    );

    const close = Effect.gen(function* () {
      const alreadyClosed = yield* Ref.getAndSet(closedRef, true);
      if (alreadyClosed) return;
      yield* connection.close.pipe(Effect.ignore);
      yield* Queue.shutdown(events);
    });
    yield* Effect.addFinalizer(() => connection.close.pipe(Effect.ignore));

    return {
      events: Stream.fromQueue(events),
      getSession: Ref.get(sessionRef),
      sendTurn,
      interruptTurn,
      compactThread,
      readThreadMessages,
      unknownEvents: Ref.get(unknownEventsRef),
      close,
      closed: isClosed,
    } satisfies PiSessionRuntimeShape;
  });
