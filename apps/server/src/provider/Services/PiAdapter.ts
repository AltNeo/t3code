/**
 * PiAdapterShape — the Pi adapter's specialization of the provider SPI.
 *
 * @module provider/PiAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export type PiAdapterShape = ProviderAdapterShape<ProviderAdapterError>;
