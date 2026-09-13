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
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  ProviderItemId,
  TurnId,
  type ProviderEvent,
  type ProviderSession,
  type ProviderInstanceId,
  type RuntimeMode,
  type RuntimeTaskStatus,
  type ProviderUserInputAnswers,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
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
  decodePiSubagentSnapshot,
  decodePiThinkingLevelChanged,
  decodePiToolExecutionEnd,
  decodePiToolExecutionStart,
  decodePiToolExecutionUpdate,
  decodePiTurnEnd,
  isPiDialogMethod,
  PI_SUBAGENT_ASYNC_WIDGET_KEY,
  piDialogCancelledResponse,
  piModelSlug,
  piSubagentChildNodes,
  piSubagentSnapshotLine,
  splitPiModelSlug,
  type PiFrame,
  type PiResponse,
  type PiSessionState,
  type PiSubagentNode,
  type PiSubagentSnapshot,
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
  readonly respondToUserInput: (
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ) => Effect.Effect<void, PiSessionRuntimeError>;
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

/** Pi tool names that spawn pi-subagents children. */
const PI_SUBAGENT_TOOL_NAMES: ReadonlySet<string> = new Set([
  "subagent",
  "task",
  "subagent_supervisor",
]);
const PI_SUBAGENT_TITLE_LIMIT = 120;
const PI_SUBAGENT_DESCRIPTION_LIMIT = 200;

const isPiSubagentTool = (toolName: string): boolean => PI_SUBAGENT_TOOL_NAMES.has(toolName);

/**
 * Snapshot state → T3's wide status vocabulary. An unrecognised state maps to
 * `running` on purpose: a newer Pi that adds a state must not have a live run
 * reported as finished.
 */
export const piTaskStatusForState = (state: string): RuntimeTaskStatus => {
  switch (state) {
    case "queued":
      return "pending";
    case "paused":
      return "waiting";
    case "idle":
      return "idle";
    case "complete":
      return "completed";
    case "stopped":
      // The wide vocabulary has no "stopped": that value exists only on
      // `task.completed`, which `piTaskCompletionForState` feeds.
      return "cancelled";
    case "failed":
    case "partial":
    case "rejected":
      return "failed";
    default:
      return "running";
  }
};

/** Terminal mapping for `task.completed`, whose status is the narrow set
 * `completed | failed | stopped`. `undefined` means the run is still live. */
export const piTaskCompletionForState = (
  state: string,
): "completed" | "failed" | "stopped" | undefined => {
  switch (state) {
    case "complete":
      return "completed";
    case "stopped":
      return "stopped";
    case "failed":
    case "partial":
    case "rejected":
      return "failed";
    default:
      return undefined;
  }
};

const boundedText = (value: string, limit: number): string =>
  value.length <= limit ? value : `${value.slice(0, limit - 1)}…`;

/**
 * Human status line for one snapshot node. Never empty: a blank description is
 * rejected by `task.progress`, and the agents panel renders this text as-is.
 */
export const piSubagentDescription = (node: PiSubagentNode): string => {
  const hostDetail = node.hostStep?.detail?.trim();
  if (hostDetail !== undefined && hostDetail.length > 0) {
    return boundedText(hostDetail, PI_SUBAGENT_DESCRIPTION_LIMIT);
  }
  if (node.activity?.state === "needs_attention") return "Needs attention";
  const tool = node.activity?.currentTool?.trim();
  const prefix = tool !== undefined && tool.length > 0 ? `Running ${tool}` : "Running";
  switch (node.state) {
    case "queued":
      return "Queued";
    case "paused":
      return "Paused";
    case "idle":
      return "Idle";
    case "complete":
      return "Completed";
    case "stopped":
      return "Stopped";
    case "failed":
      return "Failed";
    case "partial":
      return "One or more child runs failed";
    case "rejected":
      return "Rejected";
    default:
      return prefix;
  }
};

/**
 * Flattens the snapshot tree into the nodes that are runs. `step` and
 * `host-step` nodes are phases *inside* a run — the run node already repeats
 * their label and state, so emitting them as rows too would double every
 * single-child run. Steps that belong to a nested run still group under it.
 */
export const collectPiSubagentRuns = (
  nodes: ReadonlyArray<PiSubagentNode>,
  parentAgentId: string | undefined,
): ReadonlyArray<{ readonly node: PiSubagentNode; readonly parentAgentId?: string }> => {
  const collected: Array<{ readonly node: PiSubagentNode; readonly parentAgentId?: string }> = [];
  for (const node of nodes) {
    if (node.kind !== "subagent" && node.kind !== "workflow") {
      collected.push(...collectPiSubagentRuns(piSubagentChildNodes(node), parentAgentId));
      continue;
    }
    collected.push({ node, ...(parentAgentId !== undefined ? { parentAgentId } : {}) });
    collected.push(...collectPiSubagentRuns(piSubagentChildNodes(node), node.id));
  }
  return collected;
};

/** The agent name Pi puts on a `subagent` call, falling back to the tool name. */
const piSubagentCallLabel = (args: unknown, toolName: string): string => {
  if (typeof args !== "object" || args === null) return toolName;
  const agent = (args as { readonly agent?: unknown }).agent;
  return typeof agent === "string" && agent.trim().length > 0 ? agent.trim() : toolName;
};

/** `async: false` runs the child inside the parent tool call and dies with it;
 * anything else detaches the child to a background runner. */
const piToolCallIsForeground = (args: unknown): boolean =>
  typeof args === "object" &&
  args !== null &&
  (args as { readonly async?: unknown }).async === false;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Last non-empty string of a bounded tail array (`recentOutput`, `recentTools`). */
const lastNonEmptyText = (value: unknown): string | undefined => {
  if (!Array.isArray(value)) return undefined;
  for (let index = value.length - 1; index >= 0; index -= 1) {
    const candidate = value[index];
    if (typeof candidate === "string" && candidate.trim().length > 0) return candidate.trim();
  }
  return undefined;
};

interface PiForegroundSubagentProgress {
  readonly description: string;
  readonly fingerprint: string;
  readonly lastToolName?: string;
  readonly model?: string;
}

/**
 * Progress of a foreground child, read from `tool_execution_update`. Pi streams
 * the accumulated child result there (`partialResult.details.progress[0]`); no
 * snapshot frames exist for these runs.
 */
const piForegroundSubagentProgress = (
  partialResult: unknown,
): PiForegroundSubagentProgress | undefined => {
  if (!isRecord(partialResult) || !isRecord(partialResult.details)) return undefined;
  const entries = partialResult.details.progress;
  const first = Array.isArray(entries) ? entries[0] : undefined;
  if (!isRecord(first)) return undefined;
  const status = typeof first.status === "string" ? first.status : "running";
  const output = lastNonEmptyText(first.recentOutput);
  const tool = lastNonEmptyText(first.recentTools);
  const description =
    output ??
    (tool !== undefined ? `Running ${tool}` : status === "completed" ? "Completed" : "Running");
  const model =
    typeof first.model === "string" && first.model.trim().length > 0
      ? first.model.trim()
      : undefined;
  return {
    description: boundedText(description, PI_SUBAGENT_DESCRIPTION_LIMIT),
    fingerprint: `${status}|${tool ?? ""}|${output ?? ""}`,
    ...(tool !== undefined ? { lastToolName: tool } : {}),
    ...(model !== undefined ? { model } : {}),
  };
};

/**
 * Verdict for a finished foreground child. `exitCode` decides when Pi reported
 * one; `isError` is the fallback for a call that failed before producing a
 * result. The child's own final output becomes the row's summary.
 */
const piForegroundSubagentCompletion = (
  result: unknown,
  isError: boolean,
): {
  readonly status: "completed" | "failed";
  readonly summary?: string;
  readonly model?: string;
} => {
  const first =
    isRecord(result) && isRecord(result.details) && Array.isArray(result.details.results)
      ? result.details.results[0]
      : undefined;
  if (!isRecord(first)) return { status: isError ? "failed" : "completed" };
  const exitCode = typeof first.exitCode === "number" ? first.exitCode : undefined;
  const finalOutput =
    typeof first.finalOutput === "string" && first.finalOutput.trim().length > 0
      ? first.finalOutput.trim()
      : undefined;
  const model =
    typeof first.model === "string" && first.model.trim().length > 0
      ? first.model.trim()
      : undefined;
  return {
    status: isError || (exitCode !== undefined && exitCode !== 0) ? "failed" : "completed",
    ...(finalOutput !== undefined
      ? { summary: boundedText(finalOutput, PI_SUBAGENT_DESCRIPTION_LIMIT) }
      : {}),
    ...(model !== undefined ? { model } : {}),
  };
};

/** Run id of a `subagent` tool result Pi detached to a background runner. */
const piSubagentToolResultRunId = (result: unknown): string | undefined => {
  if (!isRecord(result) || !isRecord(result.details)) return undefined;
  const details = result.details;
  for (const key of ["asyncId", "runId"] as const) {
    const candidate = details[key];
    if (typeof candidate === "string" && candidate.trim().length > 0) return candidate.trim();
  }
  return undefined;
};

/** One subagent row, reconciled from snapshots (background runs) or from the
 * spawning tool call (foreground runs). */
interface PiTrackedSubagentRun {
  readonly taskId: string;
  readonly foreground: boolean;
  readonly parentAgentId?: string;
  readonly title: string;
  description: string;
  status: RuntimeTaskStatus;
  fingerprint: string;
  toolUseId?: string;
  model?: string;
  live: boolean;
}

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

    interface PendingPiDialog {
      readonly requestId: ApprovalRequestId;
      readonly piId: string;
      readonly method: "select" | "confirm" | "input" | "editor";
      readonly turnId?: TurnId;
    }
    interface DialogState {
      readonly lifecycle: "open" | "closing" | "exited";
      readonly pending: Map<string, PendingPiDialog>;
      readonly resolving: Map<string, Deferred.Deferred<void>>;
    }
    // One Ref is the arbiter for registration, claiming, and teardown. Keeping
    // lifecycle and ownership in the same atomic state prevents a late frame
    // from landing between a lifecycle check and pending-map insertion.
    const dialogStateRef = yield* Ref.make<DialogState>({
      lifecycle: "open",
      pending: new Map(),
      resolving: new Map(),
    });

    const emitDialogResolved = (pending: PendingPiDialog, answer: unknown) =>
      emitEvent({
        kind: "notification",
        threadId: options.threadId,
        method: "user-input/resolved",
        requestId: pending.requestId,
        ...(pending.turnId !== undefined ? { turnId: pending.turnId } : {}),
        payload: { answers: { [pending.piId]: answer } },
      });

    const cancelUnownedDialog = (id: string) =>
      Ref.get(dialogStateRef).pipe(
        Effect.flatMap((state) =>
          state.lifecycle === "exited"
            ? Effect.void
            : connection.notify(piDialogCancelledResponse(id)).pipe(Effect.ignore),
        ),
      );

    const registerDialog = (
      pending: PendingPiDialog,
    ): Effect.Effect<{
      readonly accepted: boolean;
      readonly lifecycle: DialogState["lifecycle"];
    }> =>
      Ref.modify(dialogStateRef, (state) => {
        if (state.lifecycle !== "open")
          return [{ accepted: false, lifecycle: state.lifecycle }, state] as [
            { readonly accepted: boolean; readonly lifecycle: DialogState["lifecycle"] },
            DialogState,
          ];
        const next = new Map(state.pending);
        next.set(pending.piId, pending);
        return [
          { accepted: true, lifecycle: "open" as const },
          { ...state, pending: next },
        ] as [
          { readonly accepted: boolean; readonly lifecycle: DialogState["lifecycle"] },
          DialogState,
        ];
      });

    const claimDialog = (
      pending: PendingPiDialog,
      resolving: Deferred.Deferred<void>,
      allowClosing = false,
    ) =>
      Ref.modify(dialogStateRef, (state) => {
        if (
          state.pending.get(pending.piId) !== pending ||
          (state.lifecycle === "exited" && !allowClosing) ||
          (state.lifecycle === "closing" && !allowClosing)
        )
          return [undefined, state] as const;
        const nextPending = new Map(state.pending);
        nextPending.delete(pending.piId);
        const nextResolving = new Map(state.resolving);
        nextResolving.set(pending.piId, resolving);
        return [
          { resolving },
          { ...state, pending: nextPending, resolving: nextResolving },
        ] as const;
      });

    const resolveDialog = (
      pending: PendingPiDialog,
      answer: unknown,
      cancelled: boolean,
      notify = true,
      allowClosing = false,
    ) =>
      Effect.gen(function* () {
        const resolving = yield* Deferred.make<void>();
        const claim = yield* claimDialog(pending, resolving, allowClosing);
        if (claim === undefined) return;
        const finish = Effect.gen(function* () {
          if (notify) {
            const frame = cancelled
              ? piDialogCancelledResponse(pending.piId)
              : pending.method === "confirm"
                ? { type: "extension_ui_response", id: pending.piId, confirmed: answer === true }
                : { type: "extension_ui_response", id: pending.piId, value: answer };
            // Claiming the dialog already makes this terminal on the T3 side.
            // Publish that fact before enqueueing the best-effort Pi reply so a
            // stalled transport cannot leave the UI pending indefinitely.
            yield* emitDialogResolved(pending, cancelled ? null : answer);
            const written = yield* connection.notify(frame).pipe(Effect.result);
            if (written._tag === "Failure") {
              // The provider reply was not accepted, but the question is still
              // closed locally; terminate the broken transport without masking
              // the resolved event with a second resolution.
              yield* connection.close.pipe(Effect.ignore);
            }
          } else {
            yield* emitDialogResolved(pending, cancelled ? null : answer);
          }
        }).pipe(
          Effect.ensuring(
            Ref.update(dialogStateRef, (state) => {
              const resolving = new Map(state.resolving);
              resolving.delete(pending.piId);
              return { ...state, resolving };
            }).pipe(Effect.andThen(Deferred.succeed(claim.resolving, undefined)), Effect.ignore),
          ),
        );
        yield* finish;
      });

    // --- Subagent trail -------------------------------------------------
    // pi-subagents mirrors its run status through `setWidget` snapshots, and
    // T3's agents panel renders `task.*` activities. This block is the bridge:
    // it tracks runs, folds each snapshot, and emits the task lifecycle.
    //
    // Frames arrive on one sequential stream fiber, so plain maps are safe here.
    const subagentRuns = new Map<string, PiTrackedSubagentRun>();
    /** Launch labels of `subagent` tool calls that have not returned yet. */
    const openSubagentCalls = new Map<string, string>();
    /** Run id → tool call id, learned when the call returns after the run was
     * first seen in a snapshot. */
    const lateBoundToolUseIds = new Map<string, string>();
    const omittedSubagentsWarnedRef = yield* Ref.make(false);

    const emitTaskEvent = (
      method: "task/started" | "task/progress" | "task/updated" | "task/completed",
      payload: Readonly<Record<string, unknown>>,
      turnId: TurnId | undefined,
    ) =>
      emitEvent({
        kind: "notification",
        threadId: options.threadId,
        method,
        ...(turnId !== undefined ? { turnId } : {}),
        payload,
      });

    /** Turn attribution only while the spawning turn is still running: a
     * detached child outlives its turn, and its completion must not be filed
     * under a turn the user already finished. */
    const subagentTurnId = Effect.gen(function* () {
      const turn = yield* Ref.get(activeTurnRef);
      return turn !== undefined && !turn.settled ? turn.turnId : undefined;
    });

    /** Identity linkage, repeated on every row so the client fold can rebuild a
     * row whose start has aged out of retention. `agentId` stays unset: that is
     * what classifies a run as an agent instead of background work. */
    const subagentLinkage = (run: PiTrackedSubagentRun): Readonly<Record<string, unknown>> => ({
      taskId: run.taskId,
      title: run.title,
      ...(run.toolUseId !== undefined ? { toolUseId: run.toolUseId } : {}),
      ...(run.parentAgentId !== undefined ? { parentAgentId: run.parentAgentId } : {}),
      ...(run.model !== undefined ? { model: run.model } : {}),
    });

    const labelMatches = (callLabel: string, runLabel: string): boolean => {
      const call = callLabel.trim().toLowerCase();
      if (call.length === 0) return false;
      return runLabel
        .split(",")
        .map((part) => part.trim().toLowerCase())
        .includes(call);
    };

    /** A background run appears in the status snapshot before its tool call
     * returns, so the first sighting can only link by launch label. */
    const bindSubagentToolUse = (runId: string, label: string): string | undefined => {
      const late = lateBoundToolUseIds.get(runId);
      if (late !== undefined) return late;
      const candidates = [...openSubagentCalls.entries()].filter(([, callLabel]) =>
        labelMatches(callLabel, label),
      );
      return candidates.length === 1 ? candidates[0]?.[0] : undefined;
    };

    const applySubagentSnapshot = (snapshot: PiSubagentSnapshot) =>
      Effect.gen(function* () {
        const omittedRuns = snapshot.omitted?.runs ?? 0;
        if (omittedRuns > 0 || snapshot.omitted?.byteLimitExceeded === true) {
          // One warning per session: the snapshot repeats ~once a second.
          const warned = yield* Ref.getAndSet(omittedSubagentsWarnedRef, true);
          if (!warned) {
            yield* emitWarning(
              "runtime/warning",
              omittedRuns > 0
                ? `${omittedRuns} subagent run(s) are missing from Pi's status snapshot, so this thread's agent list is incomplete.`
                : "Pi truncated its subagent status snapshot, so this thread's agent list is incomplete.",
            );
          }
        }
        const turnId = yield* subagentTurnId;
        yield* Effect.forEach(
          collectPiSubagentRuns(snapshot.runs, undefined),
          (entry) => reconcileSubagentRun(entry, turnId),
          { discard: true },
        );
      });

    const reconcileSubagentRun = (
      entry: { readonly node: PiSubagentNode; readonly parentAgentId?: string },
      turnId: TurnId | undefined,
    ) =>
      Effect.gen(function* () {
        const node = entry.node;
        const completion = piTaskCompletionForState(node.state);
        const attention = node.activity?.state === "needs_attention";
        const status: RuntimeTaskStatus = attention ? "waiting" : piTaskStatusForState(node.state);
        const description = piSubagentDescription(node);
        const fingerprint = `${node.state}|${node.activity?.currentTool ?? ""}|${attention ? "attention" : ""}|${node.hostStep?.state ?? ""}`;
        const existing = subagentRuns.get(node.id);

        if (existing === undefined) {
          const toolUseId = bindSubagentToolUse(node.id, node.label);
          const run: PiTrackedSubagentRun = {
            taskId: node.id,
            foreground: false,
            ...(entry.parentAgentId !== undefined ? { parentAgentId: entry.parentAgentId } : {}),
            title: boundedText(node.label, PI_SUBAGENT_TITLE_LIMIT),
            description,
            status,
            fingerprint,
            ...(toolUseId !== undefined ? { toolUseId } : {}),
            live: completion === undefined,
          };
          subagentRuns.set(node.id, run);
          yield* emitTaskEvent("task/started", { ...subagentLinkage(run), description }, turnId);
          if (completion !== undefined) {
            yield* emitTaskEvent(
              "task/completed",
              { ...subagentLinkage(run), status: completion, summary: description },
              turnId,
            );
          }
          return;
        }

        if (!existing.live) return;
        // The launch call can return after the run's first snapshot, so the
        // link to its tool row is learned late and published on the next row.
        const toolUseId = existing.toolUseId ?? bindSubagentToolUse(node.id, node.label);
        const learnedToolUse = existing.toolUseId === undefined && toolUseId !== undefined;
        if (learnedToolUse) existing.toolUseId = toolUseId;
        existing.description = description;
        existing.status = status;

        if (completion !== undefined) {
          existing.live = false;
          existing.fingerprint = fingerprint;
          yield* emitTaskEvent(
            "task/completed",
            { ...subagentLinkage(existing), status: completion, summary: description },
            turnId,
          );
          return;
        }

        const changed = existing.fingerprint !== fingerprint;
        existing.fingerprint = fingerprint;
        if (!changed && !learnedToolUse) return;
        if (status === "waiting" || status === "idle") {
          // Not running work: the panel shows a resting row, and the sidebar
          // liveness pill must not count it.
          yield* emitTaskEvent(
            "task/updated",
            { ...subagentLinkage(existing), status, description },
            turnId,
          );
          return;
        }
        const currentTool = node.activity?.currentTool;
        yield* emitTaskEvent(
          "task/progress",
          {
            ...subagentLinkage(existing),
            description,
            status,
            ...(currentTool !== undefined && currentTool.length > 0
              ? { lastToolName: currentTool }
              : {}),
          },
          turnId,
        );
      });

    /** A foreground child runs inside its tool call, so its row is driven by the
     * call itself: no snapshot frame is ever emitted for it. */
    const startForegroundSubagent = (toolCallId: string, label: string, turnId: TurnId) =>
      Effect.gen(function* () {
        const run: PiTrackedSubagentRun = {
          taskId: toolCallId,
          foreground: true,
          title: boundedText(label, PI_SUBAGENT_TITLE_LIMIT),
          description: "Running",
          status: "running",
          fingerprint: "running||",
          toolUseId: toolCallId,
          live: true,
        };
        subagentRuns.set(toolCallId, run);
        yield* emitTaskEvent(
          "task/started",
          { ...subagentLinkage(run), description: run.description },
          turnId,
        );
      });

    const updateForegroundSubagent = (toolCallId: string, turnId: TurnId, partialResult: unknown) =>
      Effect.gen(function* () {
        const run = subagentRuns.get(toolCallId);
        if (run === undefined || !run.live) return;
        const progress = piForegroundSubagentProgress(partialResult);
        if (progress === undefined) return;
        const learnedModel = run.model === undefined && progress.model !== undefined;
        if (progress.model !== undefined) run.model = progress.model;
        const changed = run.fingerprint !== progress.fingerprint;
        if (!changed && !learnedModel) return;
        run.fingerprint = progress.fingerprint;
        run.description = progress.description;
        run.status = "running";
        yield* emitTaskEvent(
          "task/progress",
          {
            ...subagentLinkage(run),
            description: progress.description,
            status: "running",
            ...(progress.lastToolName !== undefined ? { lastToolName: progress.lastToolName } : {}),
          },
          turnId,
        );
      });

    const completeForegroundSubagent = (
      toolCallId: string,
      turnId: TurnId | undefined,
      result: unknown,
      isError: boolean,
    ) =>
      Effect.gen(function* () {
        const run = subagentRuns.get(toolCallId);
        if (run === undefined || !run.live) return;
        const completion = piForegroundSubagentCompletion(result, isError);
        if (completion.model !== undefined) run.model = completion.model;
        run.live = false;
        yield* emitTaskEvent(
          "task/completed",
          {
            ...subagentLinkage(run),
            status: completion.status,
            ...(completion.summary !== undefined ? { summary: completion.summary } : {}),
          },
          turnId,
        );
      });

    /** Close live rows out as terminal. Used when the parent turn is aborted
     * (foreground children only: a detached child keeps running) and when the
     * session goes away. */
    const stopSubagentRuns = (foregroundOnly: boolean, summary: string) =>
      Effect.gen(function* () {
        const turnId = yield* subagentTurnId;
        const stopping = [...subagentRuns.values()].filter(
          (run) => run.live && (!foregroundOnly || run.foreground),
        );
        yield* Effect.forEach(
          stopping,
          (run) =>
            Effect.gen(function* () {
              run.live = false;
              yield* emitTaskEvent(
                "task/completed",
                { ...subagentLinkage(run), status: "stopped", summary },
                turnId,
              );
            }),
          { discard: true },
        );
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
        const aborted = turn.abortRequested;
        if (aborted) {
          // A foreground child runs inside its parent tool call, so aborting the
          // turn kills it. A detached child keeps running, and its row stays live.
          yield* stopSubagentRuns(true, "Stopped with the interrupted turn.");
        }
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
            if (isPiSubagentTool(tool.toolName)) {
              const label = piSubagentCallLabel(tool.args, tool.toolName);
              if (piToolCallIsForeground(tool.args)) {
                // A foreground child is bound to this call by construction, so it
                // is not a binding candidate for a snapshot run.
                yield* startForegroundSubagent(tool.toolCallId, label, activeTurn.turnId);
              } else {
                openSubagentCalls.set(tool.toolCallId, label);
              }
            }
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
            if (isPiSubagentTool(tool.toolName)) {
              yield* updateForegroundSubagent(
                tool.toolCallId,
                activeTurn.turnId,
                tool.partialResult,
              );
            }
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
            if (isPiSubagentTool(tool.toolName)) {
              if (openSubagentCalls.has(tool.toolCallId)) {
                openSubagentCalls.delete(tool.toolCallId);
              }
              const runId = piSubagentToolResultRunId(tool.result);
              if (runId !== undefined) {
                // The run was already tracked when its snapshot arrived first;
                // otherwise remember the link for the frame that follows.
                lateBoundToolUseIds.set(runId, tool.toolCallId);
                const tracked = subagentRuns.get(runId);
                if (tracked !== undefined && tracked.toolUseId === undefined && tracked.live) {
                  tracked.toolUseId = tool.toolCallId;
                  // Publish the link on its own row: the work log uses it to hide
                  // the launch-tool row that this agent row replaces.
                  const linkTurnId = yield* subagentTurnId;
                  yield* emitTaskEvent(
                    "task/progress",
                    {
                      ...subagentLinkage(tracked),
                      description: tracked.description,
                      status: tracked.status,
                    },
                    linkTurnId,
                  );
                }
              }
              yield* completeForegroundSubagent(
                tool.toolCallId,
                activeTurn.turnId,
                tool.result,
                isError,
              );
            }
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
            if (Option.isNone(decoded)) {
              const malformedId = typeof frame.id === "string" ? frame.id : undefined;
              if (malformedId !== undefined) {
                yield* cancelUnownedDialog(malformedId);
              }
              yield* emitWarning(
                "runtime/warning",
                "Malformed Pi extension UI request was cancelled.",
              );
              return;
            }
            const request = decoded.value;
            if (
              request.method === "setWidget" &&
              request.widgetKey === PI_SUBAGENT_ASYNC_WIDGET_KEY
            ) {
              // pi-subagents republishes its whole run status here (~1/s), so the
              // bridge folds snapshots instead of accumulating events.
              const line = piSubagentSnapshotLine(request.widgetLines);
              // A lineless setWidget clears the widget — at session start, during
              // compaction, and at teardown. Pi always reports a run's terminal
              // state in a snapshot first, so a clear never means "all finished".
              if (line === undefined) return;
              const snapshot = decodePiSubagentSnapshot(line);
              if (Option.isNone(snapshot)) return;
              yield* applySubagentSnapshot(snapshot.value);
              return;
            }
            if (!isPiDialogMethod(request.method)) {
              // Known fire-and-forget methods do not receive a response. An
              // unknown method with an id may nevertheless be blocking in a
              // newer Pi, so cancel it rather than leaving the process hung.
              const known = new Set([
                "notify",
                "setStatus",
                "setTitle",
                "set_editor_text",
                "setWidget",
              ]);
              if (known.has(request.method)) return;
              yield* cancelUnownedDialog(request.id);
              yield* emitWarning(
                "runtime/warning",
                `Unsupported Pi extension UI method '${request.method}' was cancelled.`,
                { method: request.method },
              );
              return;
            }
            const turnId =
              activeTurn !== undefined && !activeTurn.settled ? activeTurn.turnId : undefined;
            const requestId = ApprovalRequestId.make(request.id);
            const title = request.title?.trim() || "Pi extension question";
            const prompt = request.message?.trim() || title;
            const initialText = request.prefill;
            const placeholder = request.placeholder;
            const question = prompt;
            const dialogOptions =
              request.method === "confirm"
                ? [
                    { label: "Confirm", description: "Confirm", value: "true" },
                    { label: "Cancel", description: "Cancel", value: "false" },
                  ]
                : (request.options ?? []).map((label) => ({
                    label,
                    description: label,
                    value: label,
                  }));
            if (request.method === "select" && dialogOptions.length === 0) {
              yield* cancelUnownedDialog(request.id);
              yield* emitWarning(
                "runtime/warning",
                "Malformed Pi select dialog had no options; it was cancelled.",
              );
              return;
            }
            const pending: PendingPiDialog = {
              requestId,
              piId: request.id,
              method: request.method,
              ...(turnId !== undefined ? { turnId } : {}),
            };
            const registration = yield* registerDialog(pending);
            if (!registration.accepted) {
              // Teardown won ownership; cancel while closing, but never write
              // after exit (the process can no longer consume the frame).
              if (registration.lifecycle === "closing") {
                yield* cancelUnownedDialog(request.id);
              }
              return;
            }
            if (request.timeout !== undefined && request.timeout > 0) {
              yield* Effect.sleep(Duration.millis(request.timeout)).pipe(
                Effect.andThen(resolveDialog(pending, null, true, false)),
                Effect.forkIn(runtimeScope),
              );
            }
            yield* emitEvent({
              kind: "request",
              threadId: options.threadId,
              method: "user-input/requested",
              requestId,
              ...(turnId !== undefined ? { turnId } : {}),
              payload: {
                questions: [
                  {
                    id: request.id,
                    header: title,
                    question,
                    options: dialogOptions,
                    ...(initialText !== undefined ? { initialValue: initialText } : {}),
                    ...(placeholder !== undefined ? { placeholder } : {}),
                    allowCustomAnswer: request.method === "input" || request.method === "editor",
                  },
                ],
              },
            });
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

    const frameConsumer = yield* Stream.runForEach(connection.frames, handleFrame).pipe(
      Effect.forkIn(runtimeScope),
    );

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
          const alreadyClosed = yield* Ref.getAndSet(closedRef, true);
          if (alreadyClosed) return;
          // The connection ends frames only after stdout has been fully parsed.
          // Join the consumer before claiming dialog ownership so its final
          // frame is handled before any exit events are emitted.
          yield* Fiber.join(frameConsumer);
          // Claim ownership before draining: no subsequent dialog may become
          // pending while exit cancellation events are being emitted.
          const dialogSnapshot = yield* Ref.modify(
            dialogStateRef,
            (state) =>
              [
                { pending: [...state.pending.values()], resolving: [...state.resolving.values()] },
                { ...state, lifecycle: "exited" as const },
              ] as const,
          );
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
          // The session is gone, so nothing can report on these rows again.
          yield* stopSubagentRuns(false, "Stopped when the Pi session exited.").pipe(Effect.ignore);
          yield* Effect.forEach(
            dialogSnapshot.pending,
            (pending) => resolveDialog(pending, null, true, false, true),
            {
              discard: true,
            },
          );
          yield* Effect.forEach(dialogSnapshot.resolving, Deferred.await, { discard: true });
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
          // End only after terminal runtime events have been offered so the
          // adapter can drain them deterministically.
          yield* Queue.end(events as unknown as Queue.Enqueue<ProviderEvent, Cause.Done>);
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

    const respondToUserInput = (requestId: ApprovalRequestId, answers: ProviderUserInputAnswers) =>
      Effect.gen(function* () {
        const pending = [...(yield* Ref.get(dialogStateRef)).pending.values()].find(
          (entry) => entry.requestId === requestId,
        );
        if (pending === undefined) {
          return yield* new PiSessionRequestError({
            threadId: options.threadId,
            operation: `user-input/${requestId}`,
            detail: "Unknown pending Pi extension dialog.",
          });
        }
        const raw = answers[pending.piId];
        const cancelled = raw === null || raw === undefined;
        const answer =
          pending.method === "confirm"
            ? raw === true || raw === "true" || raw === "Confirm"
            : Array.isArray(raw)
              ? raw[0]
              : raw;
        yield* resolveDialog(pending, answer, cancelled);
      });

    const close: Effect.Effect<void> = Effect.gen(function* () {
      const alreadyClosed = yield* Ref.getAndSet(closedRef, true);
      if (alreadyClosed) return;
      // Transition and snapshot atomically; late frames are rejected and
      // cannot create an orphan question.
      const dialogSnapshot = yield* Ref.modify(
        dialogStateRef,
        (state) =>
          [
            { pending: [...state.pending.values()], resolving: [...state.resolving.values()] },
            { ...state, lifecycle: "closing" as const },
          ] as const,
      );
      // Live rows and dialogs are closed out while the queue is still open.
      yield* stopSubagentRuns(false, "Stopped when the session closed.").pipe(Effect.ignore);
      yield* Effect.forEach(
        dialogSnapshot.pending,
        (pending) => resolveDialog(pending, null, true, true, true).pipe(Effect.ignore),
        { discard: true },
      );
      yield* Effect.forEach(dialogSnapshot.resolving, Deferred.await, { discard: true });
      yield* connection.close.pipe(Effect.ignore);
      // Queue.end preserves buffered events and lets the adapter acknowledge
      // delivery by joining its event fiber.
      yield* Queue.end(events as unknown as Queue.Enqueue<ProviderEvent, Cause.Done>);
    });
    yield* Effect.addFinalizer(() => connection.close.pipe(Effect.ignore));

    return {
      events: Stream.fromQueue(events),
      getSession: Ref.get(sessionRef),
      sendTurn,
      interruptTurn,
      respondToUserInput,
      compactThread,
      readThreadMessages,
      unknownEvents: Ref.get(unknownEventsRef),
      close,
      closed: isClosed,
    } satisfies PiSessionRuntimeShape;
  });
