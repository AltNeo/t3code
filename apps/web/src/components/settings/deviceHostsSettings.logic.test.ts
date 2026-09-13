import { describe, expect, it } from "vite-plus/test";
import { updateDeviceHosts } from "./deviceHostsSettings.logic";

describe("device host changes across environments", () => {
  const shared = { id: "shared", label: "Mac mini", target: "julius@macmini" };
  const local = { id: "other", label: "Android", target: "julius@android" };

  it("adds to differing host lists without losing environment-specific hosts, including on retry", () => {
    const environments = [[], [local], [shared, local]];
    const saved = environments.map((hosts) => updateDeviceHosts(hosts, shared, false));
    expect(saved).toEqual([[shared], [local, shared], [shared, local]]);
    expect(saved.map((hosts) => updateDeviceHosts(hosts, shared, false))).toEqual(saved);
  });

  it("edits and removes the shared host while preserving unrelated hosts", () => {
    const edited = { ...shared, target: "julius@new-address" };
    const saved = [[shared], [local, shared]].map((hosts) =>
      updateDeviceHosts(hosts, edited, false),
    );
    expect(saved).toEqual([[edited], [local, edited]]);
    expect(saved.map((hosts) => updateDeviceHosts(hosts, edited, true))).toEqual([[], [local]]);
  });
});
