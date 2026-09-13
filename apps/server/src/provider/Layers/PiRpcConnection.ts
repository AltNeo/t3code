/**
 * PiRpcConnection — LF-framed JSONL transport for one `pi --mode rpc` child.
 *
 * Owns the process, the stdout line reader, request/response correlation by
 * `id`, stderr capture, and exit detection. It knows nothing about T3 turns or
 * sessions; `PiSessionRuntime` builds those on top.
 *
 * The reader is deliberately hand-rolled: `node:readline` also splits on
 * U+2028/U+2029, which are legal inside JSON strings and would corrupt records.
 *
 * @module provider/PiRpcConnection
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  decodePiResponse,
  encodePiFrame,
  isPiResponseFrame,
  parsePiFrame,
  splitPiLines,
  type PiFrame,
  type PiResponse,
} from "./piRpcProtocol.ts";

const FORCE_KILL_AFTER = "2 seconds" as const;
const STDOUT_SHUTDOWN_GRACE = "3 seconds" as const;

export class PiRpcSpawnError extends Schema.TaggedError<PiRpcSpawnError>()("PiRpcSpawnError", {
  command: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Failed to spawn Pi RPC process: ${this.command}`;
  }
}

export class PiRpcWriteError extends Schema.TaggedError<PiRpcWriteError>()("PiRpcWriteError", {
  command: Schema.String,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Failed to write Pi RPC command '${this.command}': ${this.detail}`;
  }
}

export class PiRpcProcessExitedError extends Schema.TaggedError<PiRpcProcessExitedError>()(
  "PiRpcProcessExitedError",
  {
    pid: Schema.Finite,
    code: Schema.Finite,
  },
) {
  override get message(): string {
    return `Pi RPC process ${this.pid} exited with code ${this.code}.`;
  }
}

export class PiRpcRequestError extends Schema.TaggedError<PiRpcRequestError>()(
  "PiRpcRequestError",
  {
    command: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi RPC command '${this.command}' failed: ${this.detail}`;
  }
}

export type PiRpcError =
  | PiRpcSpawnError
  | PiRpcWriteError
  | PiRpcProcessExitedError
  | PiRpcRequestError;

export interface PiProcessExit {
  readonly pid: number;
  readonly code: number;
}

export interface PiRpcConnectionOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly extendEnv: boolean;
}

export interface PiRpcConnectionShape {
  readonly pid: number;
  /** Sends a command and resolves with its `response` frame. */
  readonly request: (
    frame: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<PiResponse, PiRpcError>;
  /** Writes a frame without waiting for a response (extension UI replies). */
  readonly notify: (frame: Readonly<Record<string, unknown>>) => Effect.Effect<void, PiRpcError>;
  /** Every non-response frame, in arrival order. */
  readonly frames: Stream.Stream<PiFrame>;
  readonly stderrLines: Stream.Stream<string>;
  readonly exited: Effect.Effect<PiProcessExit>;
  readonly isClosed: Effect.Effect<boolean>;
  /** Ends stdin (Pi exits on EOF), then force-kills if it lingers. */
  readonly close: Effect.Effect<void>;
}

export const makePiRpcConnection = (
  options: PiRpcConnectionOptions,
): Effect.Effect<
  PiRpcConnectionShape,
  PiRpcSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const scope = yield* Scope.Scope;
    const exitDeferred = yield* Deferred.make<PiProcessExit>();
    const transportRef = yield* Ref.make({
      closed: false,
      pending: new Map<string, Deferred.Deferred<PiResponse, PiRpcError>>(),
    });
    const counterRef = yield* Ref.make(0);
    const frames = yield* Queue.unbounded<PiFrame>();
    const stderrQueue = yield* Queue.unbounded<string>();
    const outgoing = yield* Queue.unbounded<string, Cause.Done<void>>();

    const spawnCommand = yield* resolveSpawnCommand(options.binaryPath, options.args, {
      env: options.env,
      extendEnv: options.extendEnv,
    });
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          cwd: options.cwd,
          env: options.env,
          extendEnv: options.extendEnv,
          forceKillAfter: FORCE_KILL_AFTER,
          shell: spawnCommand.shell,
        }),
      )
      .pipe(
        Effect.mapError(
          (cause) =>
            new PiRpcSpawnError({
              command: `${options.binaryPath} ${options.args.join(" ")}`.trim(),
              cause,
            }),
        ),
      );

    const failPending = (error: PiRpcError) =>
      Effect.gen(function* () {
        const pending = yield* Ref.modify(
          transportRef,
          (state) => [[...state.pending.values()], { ...state, pending: new Map() }] as const,
        );
        yield* Effect.forEach(pending, (deferred) => Deferred.fail(deferred, error), {
          discard: true,
        });
      });

    const handleFrame = (frame: PiFrame) =>
      Effect.gen(function* () {
        if (!isPiResponseFrame(frame)) {
          yield* Queue.offer(frames, frame);
          return;
        }
        const decoded = decodePiResponse(frame);
        if (Option.isNone(decoded)) {
          yield* Effect.logDebug("Ignoring malformed Pi response frame.", { frame });
          return;
        }
        const response = decoded.value;
        if (response.id === undefined) {
          yield* Effect.logDebug("Ignoring uncorrelated Pi response frame.", {
            command: response.command,
          });
          return;
        }
        const pending = yield* Ref.get(transportRef);
        const deferred = pending.pending.get(response.id);
        if (deferred === undefined) {
          yield* Effect.logDebug("Ignoring Pi response for unknown request id.", {
            command: response.command,
          });
          return;
        }
        yield* Ref.update(transportRef, (state) => {
          const next = new Map(state.pending);
          next.delete(response.id as string);
          return { ...state, pending: next };
        });
        yield* Deferred.succeed(deferred, response);
      });

    let buffer = "";

    const stdoutDone = yield* Deferred.make<void>();

    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          buffer += chunk;
          const { lines, rest } = splitPiLines(buffer);
          buffer = rest;
          for (const line of lines) {
            const frame = parsePiFrame(line);
            if (frame === undefined) {
              yield* Effect.logDebug("Ignoring unparsable Pi stdout record.", { line });
              continue;
            }
            yield* handleFrame(frame);
          }
        }),
      ),
      Effect.onExit((exit) =>
        Effect.gen(function* () {
          if (Exit.isSuccess(exit) && buffer.trim().length > 0) {
            const frame = parsePiFrame(buffer.trim());
            buffer = "";
            if (frame !== undefined) {
              yield* handleFrame(frame);
            }
          }
          // Preserve every frame parsed from stdout before completing the
          // stream. Queue.shutdown would discard frames buffered ahead of the
          // process-exit signal (including a final blocking UI request).
          yield* Queue.end(frames as unknown as Queue.Enqueue<PiFrame, Cause.Done>);
          yield* Deferred.succeed(stdoutDone, undefined);
        }),
      ),
      Effect.forkIn(scope),
    );

    yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.runForEach((line) => Queue.offer(stderrQueue, line)),
      Effect.forkIn(scope),
    );

    yield* Stream.fromQueue(outgoing).pipe(
      Stream.encodeText,
      Stream.run(child.stdin),
      Effect.forkIn(scope),
    );

    yield* child.exitCode.pipe(
      Effect.exit,
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const code = Exit.isSuccess(exit) ? Number(exit.value) : -1;
          const info: PiProcessExit = { pid: child.pid, code };
          // Process exit can race stdout close. Wait for the reader's final
          // record (and its queue end) before publishing transport/session exit.
          yield* Deferred.await(stdoutDone);
          yield* Ref.update(transportRef, (state) => ({ ...state, closed: true }));
          yield* Queue.end(outgoing);
          yield* Deferred.succeed(exitDeferred, info);
          yield* failPending(new PiRpcProcessExitedError({ pid: child.pid, code }));
          yield* Queue.end(stderrQueue as unknown as Queue.Enqueue<string, Cause.Done>);
        }),
      ),
      Effect.forkIn(scope),
    );

    const writeFrame = (frame: Readonly<Record<string, unknown>>) =>
      Effect.gen(function* () {
        if (yield* Ref.get(transportRef).pipe(Effect.map((state) => state.closed))) {
          return yield* new PiRpcWriteError({
            command: String(frame.type ?? "unknown"),
            detail: "The Pi RPC connection is closed.",
          });
        }
        const accepted = yield* Queue.offer(outgoing, encodePiFrame(frame));
        if (!accepted) {
          return yield* new PiRpcWriteError({
            command: String(frame.type ?? "unknown"),
            detail: "The Pi RPC process is no longer accepting input.",
          });
        }
      });

    const request = (frame: Readonly<Record<string, unknown>>) =>
      Effect.gen(function* () {
        const id = yield* Ref.modify(counterRef, (current) => [String(current + 1), current + 1]);
        const deferred = yield* Deferred.make<PiResponse, PiRpcError>();
        const accepted = yield* Ref.modify(transportRef, (state) => {
          if (state.closed) return [false, state] as const;
          const next = new Map(state.pending);
          next.set(id, deferred);
          return [true, { ...state, pending: next }] as const;
        });
        if (!accepted) {
          return yield* new PiRpcWriteError({
            command: String(frame.type ?? "unknown"),
            detail: "The Pi RPC connection is closed.",
          });
        }
        const writeResult = yield* writeFrame({ ...frame, id }).pipe(Effect.result);
        if (writeResult._tag === "Failure") {
          yield* failPending(writeResult.failure);
          return yield* writeResult.failure;
        }
        return yield* Deferred.await(deferred).pipe(
          Effect.onInterrupt(() =>
            Ref.update(transportRef, (state) => {
              const next = new Map(state.pending);
              next.delete(id);
              return { ...state, pending: next };
            }),
          ),
        );
      });

    const close = Effect.gen(function* () {
      const alreadyClosed = yield* Ref.modify(
        transportRef,
        (state) => [state.closed, { ...state, closed: true }] as const,
      );
      if (alreadyClosed) return;
      // Pi exits on stdin EOF; only kill it when it does not.
      yield* Queue.end(outgoing);
      const exitedInTime = yield* Deferred.await(exitDeferred).pipe(
        Effect.timeoutOption(STDOUT_SHUTDOWN_GRACE),
        Effect.map(Option.isSome),
      );
      if (!exitedInTime) {
        yield* child.kill({ forceKillAfter: FORCE_KILL_AFTER }).pipe(Effect.ignore);
      }
      yield* failPending(new PiRpcProcessExitedError({ pid: child.pid, code: -1 }));
      yield* Queue.shutdown(frames);
      yield* Queue.shutdown(stderrQueue);
    });

    return {
      pid: child.pid,
      request,
      notify: (frame) => writeFrame(frame),
      frames: Stream.fromQueue(frames),
      stderrLines: Stream.fromQueue(stderrQueue),
      exited: Deferred.await(exitDeferred),
      isClosed: Ref.get(transportRef).pipe(Effect.map((state) => state.closed)),
      close,
    } satisfies PiRpcConnectionShape;
  });
