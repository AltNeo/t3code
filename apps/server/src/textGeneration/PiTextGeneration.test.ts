// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  PI_DEFAULT_MODEL_SLUG,
  PiSettings,
  ProviderInstanceId,
  TextGenerationError,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { writeFakeCli } from "../testUtils/fakeCli.ts";
import { buildPiProcessEnvironment } from "../provider/Layers/PiProvider.ts";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const PEER_SOURCE = NodeFS.readFileSync(
  new URL("../provider/testFixtures/piRpcMockPeer.mjs", import.meta.url),
  "utf8",
);
const GOLDEN_EVENTS = NodePath.join(
  NodePath.dirname(new URL(import.meta.url).pathname),
  "../provider/testFixtures/piGoldenNormalCompletion.jsonl",
);
const decodePiSettings = Schema.decodeSync(PiSettings);
const INSTANCE = ProviderInstanceId.make("pi");

const makePeer = (options: {
  readonly responses: Readonly<Record<string, unknown>>;
  readonly events?: string;
}) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-text-"));
  const scriptPath = NodePath.join(directory, "script.json");
  const logPath = NodePath.join(directory, "peer.log.jsonl");
  NodeFS.writeFileSync(scriptPath, JSON.stringify({ responses: options.responses }), "utf8");
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
  return {
    binaryPath,
    argv: (): ReadonlyArray<string> => {
      const record = NodeFS.readFileSync(logPath, "utf8")
        .split("\n")
        .filter((line) => line.trim().length > 0)
        .map((line) => JSON.parse(line) as Readonly<Record<string, unknown>>)
        .find((entry) => entry.kind === "start");
      return (record?.argv ?? []) as ReadonlyArray<string>;
    },
  };
};

const runTitleGeneration = (
  peerBinaryPath: string,
  model = "opencode-go/glm-5.3",
): Effect.Effect<string, TextGenerationError, never> =>
  Effect.gen(function* () {
    const textGeneration = yield* makePiTextGeneration(
      decodePiSettings({ enabled: true, binaryPath: peerBinaryPath }),
      buildPiProcessEnvironment(process.env),
    );
    const result = yield* textGeneration.generateThreadTitle({
      cwd: NodeOS.tmpdir(),
      message: "Add Pi support",
      previousTitle: undefined,
      attachments: [],
      modelSelection: createModelSelection(INSTANCE, model, [
        { id: "thinkingLevel", value: "high" },
      ]),
    });
    return result.title;
  }).pipe(Effect.provide(NodeServices.layer)) as Effect.Effect<string, TextGenerationError, never>;

it.effect("generates structured output from a scoped, extension-free Pi session", () =>
  Effect.gen(function* () {
    const peer = makePeer({
      responses: {
        prompt: { success: true },
        get_last_assistant_text: {
          success: true,
          data: { text: 'Here you go:\n```json\n{"title":"Add Pi provider"}\n```' },
        },
      },
      events: GOLDEN_EVENTS,
    });

    const title = yield* runTitleGeneration(peer.binaryPath);
    NodeAssert.equal(title, "Add Pi provider");

    const argv = peer.argv();
    NodeAssert.deepEqual(argv.slice(0, 4), ["--mode", "rpc", "--no-session", "--no-extensions"]);
    NodeAssert.ok(argv.includes("--model"));
    NodeAssert.equal(argv[argv.indexOf("--model") + 1], "opencode-go/glm-5.3");
    NodeAssert.equal(argv[argv.indexOf("--thinking") + 1], "high");
  }),
);

it.effect("leaves the model to Pi when the selection is the T3 default slug", () =>
  Effect.gen(function* () {
    const peer = makePeer({
      responses: {
        prompt: { success: true },
        get_last_assistant_text: {
          success: true,
          data: { text: '{"title":"Add Pi provider"}' },
        },
      },
      events: GOLDEN_EVENTS,
    });

    const title = yield* runTitleGeneration(peer.binaryPath, PI_DEFAULT_MODEL_SLUG);
    NodeAssert.equal(title, "Add Pi provider");

    // `pi-default` is a T3 slug, not a model id: naming it to Pi would fail.
    const argv = peer.argv();
    NodeAssert.ok(
      !argv.includes("--model"),
      `expected no --model for the default slug, got ${argv.join(" ")}`,
    );
    NodeAssert.equal(argv[argv.indexOf("--thinking") + 1], "high");
  }),
);

it.effect("fails cleanly when Pi returns no text", () =>
  Effect.gen(function* () {
    const peer = makePeer({
      responses: {
        prompt: { success: true },
        get_last_assistant_text: { success: true, data: { text: null } },
      },
      events: GOLDEN_EVENTS,
    });
    const result = yield* runTitleGeneration(peer.binaryPath).pipe(Effect.exit);
    NodeAssert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      NodeAssert.ok(String(result.cause).includes("Pi returned empty output"));
    }
  }),
);
