import type { SshDeviceHostConfig } from "@t3tools/contracts";

/** Apply one host change without replacing another environment's host list. */
export function updateDeviceHosts(
  hosts: ReadonlyArray<SshDeviceHostConfig>,
  host: SshDeviceHostConfig,
  remove: boolean,
): ReadonlyArray<SshDeviceHostConfig> {
  const existing = hosts.find((candidate) => candidate.id === host.id);
  if (remove) return hosts.filter((candidate) => candidate.id !== host.id);
  return existing
    ? hosts.map((candidate) => (candidate.id === host.id ? host : candidate))
    : [...hosts, host];
}
