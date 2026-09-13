// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";

import { PiSettings } from "@t3tools/contracts";
import { it } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  buildInitialPiProviderSnapshot,
  buildPiModelsFromProbe,
  buildPiProcessEnvironment,
  piCommandsToProviderEntries,
  piModelCapabilities,
  piProbeArgs,
} from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const enabledSettings = decodePiSettings({
  enabled: true,
  binaryPath: "/usr/local/bin/pi",
  agentDirectoryPath: "~/.pi/agent",
  sessionDirPath: "~/.pi/sessions",
  launchArgs: "--offline",
});

it("scrubs ambient Pi markers from the child environment", () => {
  const environment = buildPiProcessEnvironment(
    {
      PATH: "/usr/bin",
      CORP: "yes",
      PI_SUBAGENT_CHILD: "1",
      PI_SESSION_ID: "leaked",
      PI_CODING_AGENT_DIR: "/somewhere/else",
      AI_AGENT: "pi",
    },
    "~/.pi/agent",
  );
  NodeAssert.equal(environment.PATH, "/usr/bin");
  NodeAssert.equal(environment.CORP, "yes");
  NodeAssert.equal(environment.PI_SUBAGENT_CHILD, undefined);
  NodeAssert.equal(environment.PI_SESSION_ID, undefined);
  NodeAssert.equal(environment.AI_AGENT, undefined);
  // The configured agent directory replaces whatever the parent had.
  NodeAssert.ok(environment.PI_CODING_AGENT_DIR?.endsWith("/.pi/agent"));
  NodeAssert.equal(environment.PI_CODING_AGENT_DIR?.startsWith("/"), true);
});

it("leaves the agent directory unset when none is configured", () => {
  const environment = buildPiProcessEnvironment({ PATH: "/usr/bin", PI_MODEL: "x" }, undefined);
  NodeAssert.equal(environment.PI_CODING_AGENT_DIR, undefined);
  NodeAssert.equal(environment.PI_MODEL, undefined);
});

it("probes with an ephemeral, non-interactive RPC session", () => {
  const args = piProbeArgs(enabledSettings, { PATH: "/usr/bin" });
  NodeAssert.deepEqual(args.slice(0, 3), ["--mode", "rpc", "--no-session"]);
  // The session directory is expanded to an absolute path for the child.
  NodeAssert.equal(args[3], "--session-dir");
  NodeAssert.ok(args[4]?.startsWith("/"));
  NodeAssert.ok(args[4]?.endsWith("/.pi/sessions"));
  NodeAssert.equal(args.at(-1), "--offline");
  // The launch-args env override wins, mirroring the Codex convention.
  NodeAssert.deepEqual(
    piProbeArgs(decodePiSettings({ enabled: true }), {
      PATH: "/usr/bin",
      T3CODE_PI_LAUNCH_ARGS: "--approve --no-skills",
    }).slice(-2),
    ["--approve", "--no-skills"],
  );
});

it("maps Pi's advertised models and marks the live one as default", () => {
  const models = buildPiModelsFromProbe({
    state: {
      model: { id: "glm-5.3", name: "GLM-5.3", provider: "opencode-go" },
      thinkingLevel: "high",
    },
    thinkingLevels: ["low", "high", "max"],
    commands: [],
    models: [
      {
        id: "glm-5.3",
        name: "GLM-5.3",
        provider: "opencode-go",
        thinkingLevelMap: { off: null, low: "low", high: "high", max: null },
      },
      { id: "claude-fable-5", name: "", provider: "anthropic" },
      { id: "glm-5.3", name: "duplicate", provider: "opencode-go" },
    ],
  });

  NodeAssert.equal(models.length, 2);
  NodeAssert.equal(models[0]?.slug, "opencode-go/glm-5.3");
  NodeAssert.equal(models[0]?.subProvider, "opencode-go");
  NodeAssert.equal(models[0]?.isDefault, true);
  const optionDescriptors = models[0]?.capabilities?.optionDescriptors ?? [];
  NodeAssert.equal(optionDescriptors[0]?.id, "thinkingLevel");
  const descriptor = optionDescriptors[0];
  NodeAssert.equal(descriptor?.type, "select");
  // `max` is null in the model's map, so it is not offered.
  NodeAssert.deepEqual(
    descriptor?.type === "select" ? descriptor.options.map((option) => option.id) : [],
    ["low", "high"],
  );
  // A name-less model falls back to its slug, and is not the default.
  NodeAssert.equal(models[1]?.name, "anthropic/claude-fable-5");
  NodeAssert.equal(models[1]?.isDefault, undefined);
});

it("keeps same-named models from different upstreams tellable apart", () => {
  // Pi aggregates several upstreams, so `DeepSeek V4 Flash` can arrive twice.
  // Without a sub-provider the picker would render two identical rows.
  const models = buildPiModelsFromProbe({
    state: undefined,
    thinkingLevels: [],
    commands: [],
    models: [
      { id: "deepseek-v4-flash", name: "DeepSeek V4 Flash", provider: "deepseek" },
      { id: "deepseek-v4-flash", name: "DeepSeek V4 Flash", provider: "opencode-go" },
    ],
  });

  NodeAssert.deepEqual(
    models.map((model) => [model.name, model.subProvider]),
    [
      ["DeepSeek V4 Flash", "deepseek"],
      ["DeepSeek V4 Flash", "opencode-go"],
    ],
  );
});

it("drops the thinking descriptor when a model has no reasoning levels", () => {
  const capabilities = piModelCapabilities(
    { id: "plain", name: "Plain", provider: "local", thinkingLevelMap: { low: null } },
    ["low"],
    "low",
  );
  NodeAssert.deepEqual(capabilities.optionDescriptors, []);
});

it("splits Pi commands into T3 skills and slash commands", () => {
  const { skills, slashCommands } = piCommandsToProviderEntries([
    {
      name: "skill:pi-subagents",
      description: "Delegate work",
      source: "skill",
      sourceInfo: { path: "/home/u/.pi/skills/pi-subagents", scope: "user" },
    },
    { name: "subagents", description: "Fleet", source: "extension" },
    { name: "council", description: "Council", source: "prompt" },
    { name: "subagents", description: "duplicate", source: "extension" },
  ]);
  NodeAssert.deepEqual(skills, [
    {
      name: "pi-subagents",
      description: "Delegate work",
      path: "/home/u/.pi/skills/pi-subagents",
      enabled: true,
      scope: "user",
    },
  ]);
  NodeAssert.deepEqual(slashCommands, [
    { name: "subagents", description: "Fleet" },
    { name: "council", description: "Council" },
  ]);
});

it.effect("reports a disabled Pi without probing anything", () =>
  Effect.gen(function* () {
    const disabled = yield* buildInitialPiProviderSnapshot(decodePiSettings({}));
    NodeAssert.equal(disabled.enabled, false);
    NodeAssert.equal(disabled.status, "disabled");
    NodeAssert.equal(disabled.installed, false);
    NodeAssert.equal(disabled.message, "Pi is disabled in T3 Code settings.");
    NodeAssert.equal(disabled.displayName, "Pi");
    NodeAssert.equal(disabled.reportsContextWindow, true);
    NodeAssert.equal(disabled.supportsConversationRollback, false);
    NodeAssert.equal(disabled.showInteractionModeToggle, false);

    const pending = yield* buildInitialPiProviderSnapshot(enabledSettings);
    NodeAssert.equal(pending.enabled, true);
    NodeAssert.equal(pending.status, "warning");
    NodeAssert.equal(pending.message, "Checking Pi CLI availability...");
  }),
);
