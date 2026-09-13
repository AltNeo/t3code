/**
 * Pi RPC wire protocol (`pi --mode rpc`).
 *
 * Pi speaks JSONL over stdin/stdout: one JSON object per line, responses and
 * streamed events interleaved with no envelope, demultiplexed on `type`.
 * Framing is LF-only — the reader must not use `node:readline`, which also
 * splits on U+2028/U+2029, characters that are legal inside JSON strings.
 *
 * Only the frames this adapter consumes are modelled strictly. Everything else
 * round-trips as an opaque record so an unknown event type from a newer Pi
 * build is ignored instead of failing the session.
 *
 * @module provider/piRpcProtocol
 */
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

/**
 * One decoded stdout record. `type` is the only field every frame carries;
 * `response` frames add `id`/`command`/`success`/`data`/`error`, events add
 * their own payload fields.
 */
export type PiFrame = { readonly type: string } & Readonly<Record<string, unknown>>;

/** Serializes one command frame, including the trailing newline Pi expects. */
export const encodePiFrame = (frame: unknown): string => `${JSON.stringify(frame)}\n`;

/**
 * Splits accumulated stdout text into complete LF-delimited records plus the
 * unconsumed tail. A single trailing `\r` is stripped so CRLF input from a
 * Windows child still parses.
 */
export const splitPiLines = (
  buffer: string,
): { readonly lines: ReadonlyArray<string>; readonly rest: string } => {
  const lines: string[] = [];
  let rest = buffer;
  for (;;) {
    const index = rest.indexOf("\n");
    if (index === -1) break;
    const raw = rest.slice(0, index);
    rest = rest.slice(index + 1);
    const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
    if (line.length > 0) lines.push(line);
  }
  return { lines, rest };
};

/** Parses one record. Returns undefined for malformed or non-object lines. */
export const parsePiFrame = (line: string): PiFrame | undefined => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return undefined;
  }
  const type = (parsed as { readonly type?: unknown }).type;
  if (typeof type !== "string" || type.length === 0) {
    return undefined;
  }
  return parsed as PiFrame;
};

export const isPiResponseFrame = (frame: PiFrame): boolean => frame.type === "response";

/** `{"id"?,"type":"response","command","success","data"?,"error"?}` */
export const PiResponse = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.String),
  command: Schema.String,
  success: Schema.Boolean,
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
});
export type PiResponse = typeof PiResponse.Type;

export const decodePiResponse = Schema.decodeUnknownOption(PiResponse);

export const PiModel = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  provider: Schema.String,
  reasoning: Schema.optional(Schema.Boolean),
  input: Schema.optional(Schema.Array(Schema.String)),
  contextWindow: Schema.optional(Schema.Finite),
  maxTokens: Schema.optional(Schema.Finite),
  thinkingLevelMap: Schema.optional(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
});
export type PiModel = typeof PiModel.Type;

const decodePiModel = Schema.decodeUnknownOption(PiModel);

/** `get_state` payload. Absent `sessionFile` means the session is ephemeral. */
export const PiSessionState = Schema.Struct({
  model: Schema.optional(PiModel),
  thinkingLevel: Schema.optional(Schema.String),
  isStreaming: Schema.optional(Schema.Boolean),
  isCompacting: Schema.optional(Schema.Boolean),
  sessionFile: Schema.optional(Schema.String),
  sessionId: Schema.optional(Schema.String),
  sessionName: Schema.optional(Schema.String),
  messageCount: Schema.optional(Schema.Finite),
  pendingMessageCount: Schema.optional(Schema.Finite),
});
export type PiSessionState = typeof PiSessionState.Type;

export const decodePiSessionState = Schema.decodeUnknownOption(PiSessionState);

/** `get_available_models` payload. */
export const PiAvailableModels = Schema.Struct({
  models: Schema.Array(PiModel),
});
export type PiAvailableModels = typeof PiAvailableModels.Type;

export const decodePiAvailableModels = Schema.decodeUnknownOption(PiAvailableModels);

export const decodePiAvailableThinkingLevels = Schema.decodeUnknownOption(
  Schema.Struct({ levels: Schema.Array(Schema.String) }),
);

export const PiCommand = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
  source: Schema.optional(Schema.String),
  sourceInfo: Schema.optional(
    Schema.Struct({
      path: Schema.optional(Schema.String),
      scope: Schema.optional(Schema.String),
      origin: Schema.optional(Schema.String),
    }),
  ),
});
export type PiCommand = typeof PiCommand.Type;

export const decodePiCommands = Schema.decodeUnknownOption(
  Schema.Struct({ commands: Schema.Array(PiCommand) }),
);

export const decodePiLastAssistantText = Schema.decodeUnknownOption(
  Schema.Struct({ text: Schema.NullOr(Schema.String) }),
);

export const decodePiSessionStats = Schema.decodeUnknownOption(
  Schema.Struct({
    sessionFile: Schema.optional(Schema.String),
    totalMessages: Schema.optional(Schema.Finite),
    contextUsage: Schema.optional(
      Schema.Struct({
        tokens: Schema.optional(Schema.NullOr(Schema.Finite)),
        contextWindow: Schema.optional(Schema.NullOr(Schema.Finite)),
        percent: Schema.optional(Schema.NullOr(Schema.Finite)),
      }),
    ),
  }),
);

export const decodePiMessages = Schema.decodeUnknownOption(
  Schema.Struct({ messages: Schema.Array(Schema.Unknown) }),
);

/** `agent_end` closes one low-level run. It is not terminal: retries and queued
 * continuations can follow, so only `agent_settled` completes a T3 turn. */
export const decodePiAgentEnd = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("agent_end"),
    willRetry: Schema.optional(Schema.Boolean),
    messages: Schema.optional(Schema.Array(Schema.Unknown)),
  }),
);

export const decodePiTurnEnd = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("turn_end"),
    message: Schema.optional(Schema.Unknown),
    toolResults: Schema.optional(Schema.Array(Schema.Unknown)),
  }),
);

/** `message_update.assistantMessageEvent` — one streaming delta of a message. */
export const PiAssistantMessageEvent = Schema.Struct({
  type: Schema.String,
  contentIndex: Schema.optional(Schema.Finite),
  delta: Schema.optional(Schema.String),
  content: Schema.optional(Schema.String),
  id: Schema.optional(Schema.String),
  toolName: Schema.optional(Schema.String),
  toolCall: Schema.optional(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      arguments: Schema.optional(Schema.Unknown),
    }),
  ),
});
export type PiAssistantMessageEvent = typeof PiAssistantMessageEvent.Type;

export const decodePiMessageUpdate = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("message_update"),
    usage: Schema.optional(Schema.Unknown),
    assistantMessageEvent: PiAssistantMessageEvent,
  }),
);

export const decodePiToolExecutionStart = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("tool_execution_start"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    args: Schema.optional(Schema.Unknown),
  }),
);

export const decodePiToolExecutionUpdate = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("tool_execution_update"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    args: Schema.optional(Schema.Unknown),
    partialResult: Schema.optional(Schema.Unknown),
  }),
);

export const decodePiToolExecutionEnd = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("tool_execution_end"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    result: Schema.optional(Schema.Unknown),
    isError: Schema.optional(Schema.Boolean),
  }),
);

export const decodePiCompactionEnd = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("compaction_end"),
    reason: Schema.optional(Schema.String),
    aborted: Schema.optional(Schema.Boolean),
    willRetry: Schema.optional(Schema.Boolean),
    errorMessage: Schema.optional(Schema.String),
  }),
);

export const decodePiCompactionStart = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("compaction_start"),
    reason: Schema.optional(Schema.String),
  }),
);

export const decodePiQueueUpdate = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("queue_update"),
    steering: Schema.optional(Schema.Array(Schema.String)),
    followUp: Schema.optional(Schema.Array(Schema.String)),
  }),
);

export const decodePiSessionInfoChanged = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("session_info_changed"),
    name: Schema.String,
  }),
);

export const decodePiThinkingLevelChanged = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("thinking_level_changed"),
    level: Schema.String,
  }),
);

/** Blocking extension dialogs. Pi waits for an `extension_ui_response` with the
 * same `id`; fire-and-forget methods (`notify`, `setStatus`, …) need no reply.
 * `setWidget` is fire-and-forget too, but carries the pi-subagents status
 * snapshot in `widgetLines`, so the payload fields are modelled here. */
export const PiExtensionUiRequest = Schema.Struct({
  type: Schema.Literal("extension_ui_request"),
  id: Schema.String,
  method: Schema.String,
  title: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  options: Schema.optional(Schema.Array(Schema.String)),
  timeout: Schema.optional(Schema.Finite),
  placeholder: Schema.optional(Schema.String),
  prefill: Schema.optional(Schema.String),
  widgetKey: Schema.optional(Schema.String),
  widgetLines: Schema.optional(Schema.Array(Schema.String)),
  notifyType: Schema.optional(Schema.String),
  statusKey: Schema.optional(Schema.String),
  statusText: Schema.optional(Schema.String),
  text: Schema.optional(Schema.String),
});
export type PiExtensionUiRequest = typeof PiExtensionUiRequest.Type;

export const decodePiExtensionUiRequest = Schema.decodeUnknownOption(PiExtensionUiRequest);

const PI_DIALOG_METHODS = ["select", "confirm", "input", "editor"] as const;

/** True for the four methods that block until the client answers. */
export const isPiDialogMethod = (method: string): method is (typeof PI_DIALOG_METHODS)[number] =>
  (PI_DIALOG_METHODS as ReadonlyArray<string>).includes(method);

export const piDialogCancelledResponse = (id: string) => ({
  type: "extension_ui_response",
  id,
  cancelled: true,
});

/** pi-subagents status channel. The extension pushes a whole snapshot roughly
 * once a second while runs are live, so a consumer folds snapshots rather than
 * accumulating events. A frame without `widgetLines` clears the widget, which
 * happens at session start, during compaction, and at teardown — it never means
 * "every run finished". */
export const PI_SUBAGENT_ASYNC_WIDGET_KEY = "subagent-async";
export const PI_SUBAGENT_ASYNC_JSON_PREFIX = "PI_SUBAGENT_ASYNC_JSON:";

export const PiSubagentActivity = Schema.Struct({
  state: Schema.optional(Schema.String),
  currentTool: Schema.optional(Schema.String),
  lastActivityAt: Schema.optional(Schema.Finite),
  currentToolStartedAt: Schema.optional(Schema.Finite),
  turnCount: Schema.optional(Schema.Finite),
  toolCount: Schema.optional(Schema.Finite),
});
export type PiSubagentActivity = typeof PiSubagentActivity.Type;

/** Workflow/CI gate progress. Only the fields a status line needs are modelled;
 * the rest of the host-step payload is dropped by the decoder. */
export const PiSubagentHostStep = Schema.Struct({
  kind: Schema.optional(Schema.String),
  state: Schema.optional(Schema.String),
  verdict: Schema.optional(Schema.String),
  reasonCode: Schema.optional(Schema.String),
  detail: Schema.optional(Schema.String),
  target: Schema.optional(Schema.String),
});
export type PiSubagentHostStep = typeof PiSubagentHostStep.Type;

/** One node of the snapshot tree: a run, a workflow, or one of their steps.
 * `children` decodes lazily via `piSubagentChildNodes`: a self-referential
 * schema annotation would widen `DecodingServices` and poison the Effect
 * error channel (see `ProviderDriver.ts` on `Schema.Codec` vs `Schema`). */
export const PiSubagentNode = Schema.Struct({
  id: Schema.String,
  kind: Schema.String,
  label: Schema.String,
  state: Schema.String,
  startedAt: Schema.optional(Schema.Finite),
  updatedAt: Schema.optional(Schema.Finite),
  endedAt: Schema.optional(Schema.Finite),
  activity: Schema.optional(PiSubagentActivity),
  hostStep: Schema.optional(PiSubagentHostStep),
  children: Schema.optional(Schema.Array(Schema.Unknown)),
});
export type PiSubagentNode = typeof PiSubagentNode.Type;

/** Decodes one node nested under another, dropping anything unreadable instead
 * of failing the whole snapshot. */
export const piSubagentChildNodes = (
  node: Pick<PiSubagentNode, "children">,
): ReadonlyArray<PiSubagentNode> =>
  (node.children ?? []).flatMap((child) => {
    const decoded = Schema.decodeUnknownOption(PiSubagentNode)(child);
    return Option.isSome(decoded) ? [decoded.value] : [];
  });

export const PiSubagentSnapshot = Schema.Struct({
  kind: Schema.Literal("pi-subagents.async-status-snapshot"),
  version: Schema.Literal(1),
  generatedAt: Schema.optional(Schema.Finite),
  omitted: Schema.optional(
    Schema.Struct({
      runs: Schema.optional(Schema.Finite),
      children: Schema.optional(Schema.Finite),
      byteLimitExceeded: Schema.optional(Schema.Boolean),
    }),
  ),
  runs: Schema.Array(PiSubagentNode),
});
export type PiSubagentSnapshot = typeof PiSubagentSnapshot.Type;

/**
 * Decodes one `PI_SUBAGENT_ASYNC_JSON:` widget line. A line from another
 * extension, malformed JSON, or a payload this build does not model is ignored:
 * a status banner must never fail the session.
 */
export const decodePiSubagentSnapshot = (line: string): Option.Option<PiSubagentSnapshot> => {
  if (!line.startsWith(PI_SUBAGENT_ASYNC_JSON_PREFIX)) return Option.none();
  let parsed: unknown;
  try {
    parsed = JSON.parse(line.slice(PI_SUBAGENT_ASYNC_JSON_PREFIX.length));
  } catch {
    return Option.none();
  }
  return Schema.decodeUnknownOption(PiSubagentSnapshot)(parsed);
};

/** The snapshot widget's first `PI_SUBAGENT_ASYNC_JSON:` line, if any. */
export const piSubagentSnapshotLine = (
  widgetLines: ReadonlyArray<string> | undefined,
): string | undefined =>
  widgetLines?.find((line) => line.startsWith(PI_SUBAGENT_ASYNC_JSON_PREFIX));

/** `thinkingLevelMap` entries are `null` for levels a model does not support. */
export const supportedThinkingLevels = (
  levels: ReadonlyArray<string>,
  thinkingLevelMap: PiModel["thinkingLevelMap"],
): ReadonlyArray<string> => {
  if (thinkingLevelMap === undefined) return levels;
  return levels.filter((level) => {
    const mapped = thinkingLevelMap[level];
    return mapped === undefined ? true : mapped !== null;
  });
};

/**
 * T3 model slugs are `<provider>/<modelId>`, matching Pi's `--model` pattern.
 * An id that already contains a slash is the provider-qualified form Pi stores.
 */
export const piModelSlug = (model: { readonly provider: string; readonly id: string }): string =>
  `${model.provider}/${model.id}`;

export const splitPiModelSlug = (
  slug: string,
): { readonly provider: string; readonly modelId: string } | undefined => {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return undefined;
  return { provider: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
};

/**
 * Text blocks of an assistant message, used as a fallback when a build streams
 * no `text_delta` events. Pi messages carry an array of typed content blocks.
 */
export const assistantTextFromMessage = (message: unknown): string | undefined => {
  if (typeof message !== "object" || message === null) return undefined;
  const content = (message as { readonly content?: unknown }).content;
  if (typeof content === "string") {
    return content.trim().length > 0 ? content : undefined;
  }
  if (!Array.isArray(content)) return undefined;
  const text = content
    .flatMap((block) => {
      if (typeof block !== "object" || block === null) return [];
      const candidate = block as { readonly type?: unknown; readonly text?: unknown };
      return candidate.type === "text" && typeof candidate.text === "string"
        ? [candidate.text]
        : [];
    })
    .join("");
  return text.trim().length > 0 ? text : undefined;
};
