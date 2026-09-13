/**
 * PiTextGeneration — commit messages, PR copy, branch names, and thread titles
 * generated through a scoped `pi --mode rpc` session.
 *
 * The session is ephemeral (`--no-session`) and extension-free
 * (`--no-extensions`): this is a single prompt with a JSON answer, so ambient
 * skills, commands, and extensions would only add startup cost and variance.
 *
 * @module textGeneration/PiTextGeneration
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  PI_DEFAULT_MODEL_SLUG,
  TextGenerationError,
  type ModelSelection,
  type PiSettings,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import { expandHomePath } from "../pathExpansion.ts";
import { resolvePiLaunchArgs } from "../provider/Layers/PiAdapter.ts";
import { makePiRpcConnection } from "../provider/Layers/PiRpcConnection.ts";
import { decodePiLastAssistantText } from "../provider/Layers/piRpcProtocol.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const PI_TEXT_GENERATION_TIMEOUT_MS = 180_000;
const isTextGenerationError = Schema.is(TextGenerationError);

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPiJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const thinkingLevel = getModelSelectionStringOptionValue(modelSelection, "thinkingLevel");
      const text = yield* Effect.gen(function* () {
        const connection = yield* makePiRpcConnection({
          binaryPath: piSettings.binaryPath,
          args: [
            "--mode",
            "rpc",
            "--no-session",
            "--no-extensions",
            ...(piSettings.sessionDirPath.trim().length > 0
              ? ["--session-dir", expandHomePath(piSettings.sessionDirPath.trim())]
              : []),
            // `pi-default` is a T3-side slug meaning "keep the model Pi is
            // configured with"; handing it to Pi as a model id would fail.
            ...(modelSelection.model === PI_DEFAULT_MODEL_SLUG
              ? []
              : ["--model", modelSelection.model]),
            ...(thinkingLevel !== undefined ? ["--thinking", thinkingLevel] : []),
            ...resolvePiLaunchArgs(piSettings.launchArgs, environment),
          ],
          cwd,
          env: environment,
          extendEnv: false,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: "Failed to start Pi for text generation.",
                cause,
              }),
          ),
        );
        yield* Effect.addFinalizer(() => connection.close.pipe(Effect.ignore));

        const accepted = yield* connection.request({ type: "prompt", message: prompt }).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: "Pi did not accept the text generation prompt.",
                cause,
              }),
          ),
        );
        if (!accepted.success) {
          return yield* new TextGenerationError({
            operation,
            detail: `Pi rejected the text generation prompt: ${accepted.error ?? "unknown error"}`,
          });
        }

        // `agent_settled` — not `agent_end` — is the terminal watermark: Pi may
        // still retry or compact after `agent_end`.
        yield* connection.frames.pipe(
          Stream.filter((frame) => frame.type === "agent_settled"),
          Stream.runHead,
        );

        const lastText = yield* connection.request({ type: "get_last_assistant_text" }).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: "Pi did not return its final assistant text.",
                cause,
              }),
          ),
        );
        const decoded = decodePiLastAssistantText(lastText.data);
        return Option.isSome(decoded) ? (decoded.value.text ?? "").trim() : "";
      }).pipe(
        Effect.timeoutOption(PI_TEXT_GENERATION_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(new TextGenerationError({ operation, detail: "Pi request timed out." })),
            onSome: (value) => Effect.succeed(value),
          }),
        ),
      );

      if (text.length === 0) {
        return yield* new TextGenerationError({
          operation,
          detail: "Pi returned empty output.",
        });
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(text)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Pi returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Pi text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
