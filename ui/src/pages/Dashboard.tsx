// The "what's on fire" screen — broad, shallow, cross-broker. Read-only:
// per-broker detail (and the "Inspect" drill-down into a resource) lives
// on that broker's own tab now, not here — see the resolved usage-pattern
// decision in /docs/architecture.md §7 for why both a broad overview and
// a deep drill-down are first-class, just split across tabs rather than
// crammed onto one page.

import { useMemo, useState } from "react";
import type { SystemType } from "../api";
import { SystemTypeBadge } from "../components/Badges";
import StatsSummary from "../components/StatsSummary";
import { useMonitoringData } from "../context/MonitoringDataContext";

export default function Dashboard() {
  const { brokers, resources, health, loading } = useMonitoringData();
  const [environmentFilter, setEnvironmentFilter] = useState<string>("all");
  const [systemTypeFilter, setSystemTypeFilter] = useState<SystemType | "all">("all");

  const environments = useMemo(() => Array.from(new Set(brokers.map((b) => b.environment))).sort(), [brokers]);
  const brokerById = useMemo(() => Object.fromEntries(brokers.map((b) => [b.id, b])), [brokers]);

  const filteredResources = resources.filter((r) => {
    const broker = brokerById[r.broker_id];
    if (systemTypeFilter !== "all" && r.system_type !== systemTypeFilter) return false;
    if (environmentFilter !== "all" && broker?.environment !== environmentFilter) return false;
    return true;
  });

  return (
    <div>
      <StatsSummary brokers={brokers} resources={resources} health={health} />

      <section className="filters">
        <label>
          Environment:{" "}
          <select value={environmentFilter} onChange={(e) => setEnvironmentFilter(e.target.value)}>
            <option value="all">All</option>
            {environments.map((env) => (
              <option key={env} value={env}>
                {env}
              </option>
            ))}
          </select>
        </label>
        <label>
          System:{" "}
          <select value={systemTypeFilter} onChange={(e) => setSystemTypeFilter(e.target.value as SystemType | "all")}>
            <option value="all">All</option>
            <option value="kafka">Kafka</option>
            <option value="solace">Solace</option>
            <option value="mq">IBM MQ</option>
          </select>
        </label>
      </section>

      <div className="table-scroll">
        <table className="resource-table">
          <thead>
            <tr>
              <th>System</th>
              <th>Broker</th>
              <th>Namespace</th>
              <th>Name</th>
              <th>Depth</th>
              <th>Consumer lag</th>
            </tr>
          </thead>
          <tbody>
            {filteredResources.map((r) => (
              <tr key={r.id}>
                <td>
                  <SystemTypeBadge systemType={r.system_type} />
                </td>
                <td>{brokerById[r.broker_id]?.name ?? r.broker_id}</td>
                <td className="mono">{r.namespace}</td>
                <td className="mono">{r.name}</td>
                <td className="num">{r.depth_current === null ? "—" : `${r.depth_current}${r.depth_max ? ` / ${r.depth_max}` : ""}`}</td>
                <td className="num">{r.consumer_lag === null ? "—" : r.consumer_lag}</td>
              </tr>
            ))}
            {filteredResources.length === 0 && !loading && (
              <tr>
                <td colSpan={6} className="empty-state">
                  Nothing matches this filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
