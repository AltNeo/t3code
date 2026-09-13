import type { SshDeviceHostConfig } from "@t3tools/contracts";

/** Apply one host change without replacing another environment's host list. */
export function updateDeviceHosts(
  hosts: ReadonlyArray<SshDeviceHostConfig>,
  host: SshDeviceHostConfig,
  remove: boolean,
  original = host,
): ReadonlyArray<SshDeviceHostConfig> {
  const sameDestination = (candidate: SshDeviceHostConfig, other: SshDeviceHostConfig) =>
    candidate.target === other.target &&
    candidate.port === other.port &&
    candidate.identityFile === other.identityFile;
  const matches = (candidate: SshDeviceHostConfig) =>
    candidate.id === original.id || sameDestination(candidate, original);
  if (remove) return hosts.filter((candidate) => !matches(candidate));
  // A retry can encounter the updated destination on an environment that
  // already saved, including one with a different environment-local host ID.
  const existing =
    hosts.find(matches) ?? hosts.find((candidate) => sameDestination(candidate, host));
  return existing
    ? hosts.map((candidate) =>
        candidate.id === existing.id ? { ...host, id: existing.id } : candidate,
      )
    : [...hosts, host];
}
