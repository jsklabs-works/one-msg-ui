import type { HealthEvent, HealthSeverity } from "./api";

const SEVERITY_RANK: Record<HealthSeverity, number> = { ok: 0, warn: 1, critical: 2 };

/** The single worst severity among a set of health events, defaulting to
 * "ok" when there are none — absence of bad news is not itself bad news. */
export function worstSeverity(events: HealthEvent[]): HealthSeverity {
  return events.reduce<HealthSeverity>(
    (worst, e) => (SEVERITY_RANK[e.severity] > SEVERITY_RANK[worst] ? e.severity : worst),
    "ok",
  );
}

export function healthForBroker(events: HealthEvent[], brokerId: string): HealthEvent[] {
  return events.filter((e) => e.broker_id === brokerId);
}
