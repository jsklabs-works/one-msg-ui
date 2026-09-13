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

// What to actually call a resource in the UI — never the internal
// unified-model name "Resource" itself (that's the schema term shared
// across all three adapters, see /docs/architecture.md §3, not a word
// to show a Kafka user). Resource.kind already carries the real answer
// ("topic" for Kafka, "queue" for Solace/MQ — see
// api/src/api/normalize.py), so this reads off that field instead of
// guessing from system type, which also keeps working correctly if
// Solace ever surfaces a topic-kind resource alongside its queues.
const KIND_LABELS: Record<string, { singular: string; plural: string }> = {
  topic: { singular: "Topic", plural: "Topics" },
  queue: { singular: "Queue", plural: "Queues" },
};

export function kindLabel(kind: string, plural = false): string {
  const entry = KIND_LABELS[kind];
  if (!entry) return kind; // an unknown kind is shown verbatim rather than guessed at
  return plural ? entry.plural : entry.singular;
}
