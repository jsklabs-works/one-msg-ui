// The "hero numbers" row per the dataviz skill's stat-tile contract:
// label (sentence case, no trailing colon) + value, colored by status
// only where the number itself is a status (issue counts), never for
// plain magnitude. Derived entirely from data the page already fetched
// — no extra API calls.

import type { Broker, HealthEvent, Resource } from "../api";
import { worstSeverity } from "../healthUtils";

function StatTile({ label, value, sub, status }: { label: string; value: string; sub?: string; status?: "good" | "warning" | "critical" }) {
  return (
    <div className="stat-tile">
      <div className="stat-tile-label">{label}</div>
      <div className={`stat-tile-value${status ? ` status-${status}` : ""}`}>{value}</div>
      {sub && <div className="stat-tile-sub">{sub}</div>}
    </div>
  );
}

export default function StatsSummary({ brokers, resources, health }: { brokers: Broker[]; resources: Resource[]; health: HealthEvent[] }) {
  const brokersUp = brokers.filter((b) => b.status === "up").length;
  const brokersStatus: "good" | "warning" | "critical" = brokersUp === brokers.length ? "good" : brokersUp === 0 ? "critical" : "warning";

  const issues = health.filter((h) => h.severity !== "ok");
  const worstIssue = worstSeverity(issues);
  const issuesStatus: "good" | "warning" | "critical" = issues.length === 0 ? "good" : worstIssue === "critical" ? "critical" : "warning";

  const totalLag = resources.reduce((sum, r) => sum + (r.consumer_lag ?? 0), 0);
  const lagResourceCount = resources.filter((r) => r.consumer_lag !== null).length;

  return (
    <section className="stats-summary">
      <StatTile label="Brokers online" value={`${brokersUp}/${brokers.length}`} status={brokersStatus} />
      <StatTile label="Resources monitored" value={String(resources.length)} sub={`across ${brokers.length} broker(s)`} />
      <StatTile label="Health issues" value={String(issues.length)} status={issuesStatus} sub={issues.length === 0 ? "all clear" : undefined} />
      <StatTile
        label="Consumer lag"
        value={lagResourceCount > 0 ? totalLag.toLocaleString() : "—"}
        sub={lagResourceCount > 0 ? `across ${lagResourceCount} topic(s)` : "no Kafka topics yet"}
      />
    </section>
  );
}
