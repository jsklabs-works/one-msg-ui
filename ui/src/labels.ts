import type { SystemType } from "./api";

// The two-level scoping the architecture doc warns not to flatten away —
// Solace's Message VPN is a real grouping layer, not just a display
// column. Named per system since the concept means something different
// each place: a VPN, a queue manager, a cluster.
export const NAMESPACE_LABELS: Record<SystemType, string> = {
  kafka: "Cluster",
  solace: "VPN",
  mq: "Queue manager",
};
