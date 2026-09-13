import type { SystemType } from "./api";

// The two-level scoping the architecture doc warns not to flatten away —
// Solace's Message VPN is a real grouping layer, not just a display
// column. Named per system since the concept means something different
// each place: a VPN, a queue manager, a cluster.
export const NAMESPACE_LABELS: Record<SystemType, string> = {
  kafka: "Cluster",
  solace: "VPN",
  mq: "Queue manager",
  rabbitmq: "Vhost",
  activemq: "Address",
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

// NAMESPACE_LABELS reads naturally as a heading ("VPN: default"), but
// mid-sentence ("2 queues across 3 VPNs") needs the plural, lowercased
// unless it's an acronym — pluralizing "VPN" to "vpns" would un-acronym
// it, so only non-all-caps labels get lowercased. A trailing s/x/z/ch/sh
// needs "es", not another bare "s" — found live: ActiveMQ's "Address"
// came out "addresss" before this check existed.
export function namespaceLabelPlural(type: SystemType): string {
  const label = NAMESPACE_LABELS[type];
  const midSentence = label === label.toUpperCase() ? label : label.charAt(0).toLowerCase() + label.slice(1);
  const suffix = /[sxz]$|[cs]h$/.test(midSentence) ? "es" : "s";
  return `${midSentence}${suffix}`;
}
