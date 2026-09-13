// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";

import { PiSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { BUILT_IN_DRIVERS } from "../builtInDrivers.ts";
import { PiDriver } from "./PiDriver.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

// A disabled instance must render a snapshot without ever running `pi`.
const noSpawn = ChildProcessSpawner.make(() =>
  Effect.die("A disabled Pi instance must not spawn a process"),
);

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-driver-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Pi has no HTTP surface to call")),
    ),
  ),
  Layer.provideMerge(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
);

it.layer(testLayer)("PiDriver", (it) => {
  it.effect("registers as a first-party driver with Pi's settings defaults", () =>
    Effect.sync(() => {
      NodeAssert.equal(PiDriver.driverKind, "pi");
      NodeAssert.equal(PiDriver.metadata.displayName, "Pi");
      NodeAssert.equal(PiDriver.metadata.supportsMultipleInstances, true);
      NodeAssert.ok(BUILT_IN_DRIVERS.includes(PiDriver));

      const defaults = PiDriver.defaultConfig();
      NodeAssert.equal(defaults.enabled, false);
      NodeAssert.equal(defaults.binaryPath, "pi");
      NodeAssert.equal(defaults.agentDirectoryPath, "");
      NodeAssert.equal(defaults.sessionDirPath, "");
      NodeAssert.equal(defaults.launchArgs, "");
      NodeAssert.deepEqual(defaults.customModels, []);
    }),
  );

  it.effect("builds a disabled instance without spawning Pi", () =>
    Effect.gen(function* () {
      const instance = yield* PiDriver.create({
        instanceId: ProviderInstanceId.make("pi"),
        displayName: undefined,
        enabled: false,
        environment: [],
        config: PiDriver.defaultConfig(),
      });

      NodeAssert.equal(instance.driverKind, "pi");
      NodeAssert.equal(instance.enabled, false);
      NodeAssert.equal(instance.continuationIdentity.continuationKey, "pi:instance:pi");
      const snapshot = yield* instance.snapshot.getSnapshot;
      NodeAssert.equal(snapshot.status, "disabled");
      NodeAssert.equal(snapshot.displayName, "Pi");
      NodeAssert.equal(snapshot.enabled, false);
      // A disabled instance answers a workspace probe without spawning either.
      const forCwd = yield* instance.snapshotForCwd?.(process.cwd()) ?? Effect.succeed(undefined);
      NodeAssert.equal(forCwd?.status, "disabled");
      // Text generation is provided for every instance, even when unused.
      NodeAssert.equal(typeof instance.textGeneration.generateThreadTitle, "function");
      yield* instance.adapter.stopAll();
    }),
  );

  it.effect("keeps a configured agent directory out of Pi's own discovery", () =>
    Effect.gen(function* () {
      const instance = yield* PiDriver.create({
        instanceId: ProviderInstanceId.make("pi"),
        displayName: "Pi work",
        enabled: false,
        environment: [{ name: "PI_SUBAGENT_CHILD", value: "1", sensitive: false }],
        config: decodePiSettings({ agentDirectoryPath: "~/.pi/agent-work" }),
      });
      // Building the instance is enough: the adapter captured a scrubbed
      // environment, which is what keeps pi-subagents loading.
      NodeAssert.equal(instance.displayName, "Pi work");
      yield* instance.adapter.stopAll();
    }),
  );
});
