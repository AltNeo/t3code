/**
 * PiProvider — snapshot source for the Pi driver (version, models, skills).
 *
 * The probe opens a short-lived read-only RPC session (`--no-session`) and asks
 * Pi about itself. It never calls `set_model`, `set_auto_*`, or any setter:
 * several Pi setters write to the user's global config, and a health check must
 * not mutate the machine it is inspecting.
 *
 * @module provider/PiProvider
 */
import {
  type CustomModelSetting,
  type ModelCapabilities,
  type PiSettings,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { expandHomePath } from "../../pathExpansion.ts";
import { resolvePiLaunchArgs } from "../Layers/PiAdapter.ts";
import { makePiRpcConnection, type PiRpcConnectionShape } from "../Layers/PiRpcConnection.ts";
import {
  decodePiAvailableModels,
  decodePiAvailableThinkingLevels,
  decodePiCommands,
  decodePiSessionState,
  piModelSlug,
  supportedThinkingLevels,
  type PiCommand,
  type PiModel,
  type PiSessionState,
} from "../Layers/piRpcProtocol.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  supportsConversationRollback: false,
  showInteractionModeToggle: false,
  // Pi reports `contextUsage` through get_session_stats.
  reportsContextWindow: true,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });
const VERSION_PROBE_TIMEOUT_MS = 4_000;
const RPC_PROBE_TIMEOUT_MS = 15_000;
const SKILLS_PROBE_TIMEOUT_MS = 20_000;

/**
 * Pi's own var name for the agent directory. T3 maps a configured directory
 * onto it instead of copying the directory, so extensions, skills, models, and
 * credentials stay owned by Pi.
 */
export const PI_AGENT_DIRECTORY_ENV = "PI_CODING_AGENT_DIR";

/**
 * Environment for a T3-hosted Pi process. Ambient `PI_*` markers must go: a Pi
 * spawned from inside another Pi session inherits `PI_SUBAGENT_CHILD=1`, which
 * makes `pi-subagents` refuse to load, and stale `PI_SESSION_*` values leak into
 * the session's own bash tool.
 */
export const buildPiProcessEnvironment = (
  environment: NodeJS.ProcessEnv,
  agentDirectoryPath?: string,
): NodeJS.ProcessEnv => {
  const scrubbed: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(environment)) {
    if (key.startsWith("PI_") || key === "AI_AGENT") continue;
    scrubbed[key] = value;
  }
  const directory = agentDirectoryPath?.trim();
  if (directory !== undefined && directory.length > 0) {
    scrubbed[PI_AGENT_DIRECTORY_ENV] = expandHomePath(directory);
  }
  return scrubbed;
};

export interface PiProbeSnapshot {
  readonly state: PiSessionState | undefined;
  readonly models: ReadonlyArray<PiModel>;
  readonly thinkingLevels: ReadonlyArray<string>;
  readonly commands: ReadonlyArray<PiCommand>;
}

/** A read-only probe failed. Typed so callers can tell a missing binary from a
 * Pi that started but refused to answer. */
export class PiProbeError extends Schema.TaggedError<PiProbeError>()("PiProbeError", {
  operation: Schema.String,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Pi probe '${this.operation}' failed: ${this.detail}`;
  }
}

export interface PiProbeOptions {
  readonly settings: PiSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly cwd: string;
  /** Ask for project-scoped commands, so workspace skills are included. */
  readonly includeCommands?: boolean;
}

export const piProbeArgs = (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv,
): ReadonlyArray<string> => [
  "--mode",
  "rpc",
  "--no-session",
  ...(settings.sessionDirPath.trim().length > 0
    ? ["--session-dir", expandHomePath(settings.sessionDirPath.trim())]
    : []),
  ...resolvePiLaunchArgs(settings.launchArgs, environment),
];

const requestData = (
  connection: PiRpcConnectionShape,
  frame: Readonly<Record<string, unknown>>,
  operation: string,
) =>
  connection.request(frame).pipe(
    Effect.mapError((cause) => new PiProbeError({ operation, detail: cause.message, cause })),
    Effect.flatMap((response) =>
      response.success
        ? Effect.succeed(response.data)
        : Effect.fail(
            new PiProbeError({
              operation,
              detail: response.error ?? "Pi reported an unspecified failure.",
            }),
          ),
    ),
  );

/**
 * Runs one read-only RPC probe. The connection is scoped to this effect, so the
 * child is closed on every exit path, including timeouts.
 */
export const probePiSession = (
  options: PiProbeOptions,
): Effect.Effect<PiProbeSnapshot, PiProbeError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const connection = yield* makePiRpcConnection({
      binaryPath: options.settings.binaryPath,
      args: piProbeArgs(options.settings, options.environment),
      cwd: options.cwd,
      env: options.environment,
      extendEnv: false,
    }).pipe(
      Effect.mapError(
        (cause) => new PiProbeError({ operation: "spawn", detail: cause.message, cause }),
      ),
    );
    yield* Effect.addFinalizer(() => connection.close.pipe(Effect.ignore));

    const stateData = yield* requestData(connection, { type: "get_state" }, "get_state");
    const stateDecoded = decodePiSessionState(stateData);
    const modelsData = yield* requestData(
      connection,
      { type: "get_available_models" },
      "get_available_models",
    ).pipe(Effect.orElseSucceed(() => undefined));
    const models = Option.match(decodePiAvailableModels(modelsData), {
      onNone: () => [],
      onSome: (value) => value.models,
    });
    const levelsData = yield* requestData(
      connection,
      { type: "get_available_thinking_levels" },
      "get_available_thinking_levels",
    ).pipe(Effect.orElseSucceed(() => undefined));
    const thinkingLevels = Option.match(decodePiAvailableThinkingLevels(levelsData), {
      onNone: () => [],
      onSome: (value) => value.levels,
    });
    const commands = options.includeCommands
      ? Option.match(
          decodePiCommands(
            yield* requestData(connection, { type: "get_commands" }, "get_commands").pipe(
              Effect.orElseSucceed(() => undefined),
            ),
          ),
          { onNone: () => [], onSome: (value) => value.commands },
        )
      : [];

    return {
      state: Option.getOrUndefined(stateDecoded),
      models,
      thinkingLevels,
      commands,
    } satisfies PiProbeSnapshot;
  }).pipe(Effect.scoped);

/** Thinking-level choices for one model, from the levels Pi advertises and the
 * per-model map that nulls out levels a model does not support. */
export const piModelCapabilities = (
  model: PiModel,
  thinkingLevels: ReadonlyArray<string>,
  currentLevel: string | undefined,
): ModelCapabilities => {
  const levels = supportedThinkingLevels(thinkingLevels, model.thinkingLevelMap);
  if (levels.length === 0) return EMPTY_CAPABILITIES;
  const options = levels.map((level) => ({
    id: level,
    label: level,
    ...(level === (currentLevel ?? levels[0]) ? { isDefault: true } : {}),
  }));
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinkingLevel",
        label: "Thinking",
        type: "select",
        options,
        ...(currentLevel !== undefined && levels.includes(currentLevel)
          ? { currentValue: currentLevel }
          : {}),
      },
    ],
  });
};

export const buildPiModelsFromProbe = (
  snapshot: PiProbeSnapshot,
): ReadonlyArray<ServerProviderModel> => {
  const liveSlug =
    snapshot.state?.model !== undefined ? piModelSlug(snapshot.state.model) : undefined;
  const seen = new Set<string>();
  return snapshot.models.flatMap((model): ServerProviderModel[] => {
    const slug = piModelSlug(model);
    if (seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: model.name.trim().length > 0 ? model.name : slug,
        isCustom: false,
        ...(slug === liveSlug ? { isDefault: true } : {}),
        capabilities: piModelCapabilities(
          model,
          snapshot.thinkingLevels,
          snapshot.state?.thinkingLevel,
        ),
      },
    ];
  });
};

const normalizeCommandName = (name: string): string =>
  name.startsWith("skill:") ? name.slice("skill:".length) : name;

/** `get_commands` is Pi's own inventory: extension commands, prompt templates,
 * and skills. Skills become T3 skills; everything else becomes a slash command. */
export const piCommandsToProviderEntries = (
  commands: ReadonlyArray<PiCommand>,
): {
  readonly skills: ReadonlyArray<ServerProviderSkill>;
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
} => {
  const skills: ServerProviderSkill[] = [];
  const slashCommands: ServerProviderSlashCommand[] = [];
  const seenSkills = new Set<string>();
  const seenCommands = new Set<string>();
  for (const command of commands) {
    const name = normalizeCommandName(command.name).trim();
    if (name.length === 0) continue;
    if (command.source === "skill") {
      if (seenSkills.has(name)) continue;
      seenSkills.add(name);
      skills.push({
        name,
        ...(command.description ? { description: command.description } : {}),
        path: command.sourceInfo?.path ?? name,
        enabled: true,
        ...(command.sourceInfo?.scope ? { scope: command.sourceInfo.scope } : {}),
      });
      continue;
    }
    if (seenCommands.has(name)) continue;
    seenCommands.add(name);
    slashCommands.push({
      name,
      ...(command.description ? { description: command.description } : {}),
    });
  }
  return { skills, slashCommands };
};

const piModelsFromSettings = (
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> =>
  providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings.customModels);
    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Pi CLI availability...",
      },
    });
  });
}

export const checkPiProviderStatus = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.Effect<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = piModelsFromSettings(piSettings.customModels);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }

    const versionResult = yield* resolveSpawnCommand(piSettings.binaryPath, ["--version"], {
      env: environment,
    }).pipe(
      Effect.flatMap((spawnCommand) =>
        spawnAndCollect(
          piSettings.binaryPath,
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            env: environment,
            shell: spawnCommand.shell,
          }),
        ),
      ),
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      yield* Effect.logWarning("Pi CLI health check failed.", { errorTag: error._tag });
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: piSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "Pi is not installed, or `pi` is not on PATH."
            : "Failed to execute the Pi CLI health check.",
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: piSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Pi is installed but timed out while running `pi --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);

    const probe = yield* probePiSession({
      settings: piSettings,
      environment,
      cwd: cwd ?? process.cwd(),
      includeCommands: true,
    }).pipe(Effect.timeoutOption(RPC_PROBE_TIMEOUT_MS), Effect.result);
    if (Result.isFailure(probe) || Option.isNone(probe.success)) {
      const detail = Result.isFailure(probe)
        ? probe.failure.message
        : "Pi did not answer the RPC probe in time.";
      yield* Effect.logWarning("Pi RPC probe failed.", { detail });
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: piSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown" },
          message: `Pi is installed but did not start an RPC session: ${detail}`,
        },
      });
    }

    const probed = probe.success.value;
    const { skills, slashCommands } = piCommandsToProviderEntries(probed.commands);
    const discovered = buildPiModelsFromProbe(probed);
    const models =
      discovered.length > 0
        ? piModelsFromSettings(piSettings.customModels, discovered)
        : fallbackModels;

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models,
      skills,
      slashCommands,
      probe: {
        installed: true,
        version,
        // Pi starts without configured credentials, so an empty catalog is a
        // warning about the model picker, not a broken provider.
        status: discovered.length > 0 ? "ready" : "warning",
        auth: { status: "unknown" },
        ...(discovered.length === 0
          ? {
              message:
                "Pi is installed but reports no models. Check Pi's provider credentials with `pi auth check`.",
            }
          : {}),
      },
    });
  });

/** Workspace-scoped skills and slash commands, resolved per thread cwd. */
export const probePiSkillsForCwd = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
): Effect.Effect<
  {
    readonly skills: ReadonlyArray<ServerProviderSkill>;
    readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  },
  PiProbeError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  probePiSession({
    settings: piSettings,
    environment,
    cwd,
    includeCommands: true,
  }).pipe(
    Effect.map((snapshot) => piCommandsToProviderEntries(snapshot.commands)),
    Effect.timeout(SKILLS_PROBE_TIMEOUT_MS),
    Effect.catchTag("TimeoutError", () => Effect.succeed({ skills: [], slashCommands: [] })),
  );
