// The "what's on fire" screen — broad, shallow, cross-broker. One row per
// broker, not per topic/queue: a real deployment can have hundreds of
// those, and a flat list of every one of them defeats the point of a
// glance-able overview (see the health-events design for the same idea —
// per-object detail already lives one level down, on a health *event*
// or, here, on the broker's own tab). Per-broker detail (and the
// "Inspect" drill-down into a single object) lives on that broker's own
// tab now, not here — see the resolved usage-pattern decision in
// /docs/architecture.md §7 for why both a broad overview and a deep
// drill-down are first-class, just split across tabs rather than
// crammed onto one page.

import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import type { SystemType } from "../api";
import { SeverityBadge, SystemTypeBadge } from "../components/Badges";
import StatsSummary from "../components/StatsSummary";
import { useMonitoringData } from "../context/MonitoringDataContext";
import { healthForBroker, worstSeverity } from "../healthUtils";
import { kindLabel, namespaceLabelPlural } from "../labels";

export default function Dashboard() {
  const { brokers, resources, health, loading } = useMonitoringData();
  const [environmentFilter, setEnvironmentFilter] = useState<string>("all");
  const [systemTypeFilter, setSystemTypeFilter] = useState<SystemType | "all">("all");

  const environments = useMemo(() => Array.from(new Set(brokers.map((b) => b.environment))).sort(), [brokers]);

  const filteredBrokers = brokers.filter((b) => {
    if (systemTypeFilter !== "all" && b.system_type !== systemTypeFilter) return false;
    if (environmentFilter !== "all" && b.environment !== environmentFilter) return false;
    return true;
  });

  // One summary per broker — count of objects (topics/queues) and the
  // namespaces (VPNs/queue managers/clusters) they're spread across,
  // total consumer lag, and the worst health severity reported. Nothing
  // here needs the object list itself; that's what the broker's own tab
  // is for.
  const summaries = useMemo(
    () =>
      filteredBrokers.map((broker) => {
        const brokerResources = resources.filter((r) => r.broker_id === broker.id);
        const objectCount = brokerResources.length;
        const namespaceCount = new Set(brokerResources.map((r) => r.namespace)).size;
        const objectKind = brokerResources[0]?.kind;
        const lagResources = brokerResources.filter((r) => r.consumer_lag !== null);
        const totalLag = lagResources.reduce((sum, r) => sum + (r.consumer_lag ?? 0), 0);
        return {
          broker,
          objectCount,
          namespaceCount,
          objectLabel: objectKind ? kindLabel(objectKind, objectCount !== 1).toLowerCase() : "objects",
          totalLag: lagResources.length > 0 ? totalLag : null,
          severity: worstSeverity(healthForBroker(health, broker.id)),
        };
      }),
    [filteredBrokers, resources, health],
  );

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
              <th>Environment</th>
              <th>Objects</th>
              <th>Consumer lag</th>
              <th>Health</th>
            </tr>
          </thead>
          <tbody>
            {summaries.map(({ broker, objectCount, namespaceCount, objectLabel, totalLag, severity }) => (
              <tr key={broker.id}>
                <td>
                  <SystemTypeBadge systemType={broker.system_type} />
                </td>
                <td>
                  <Link to={`/brokers/${encodeURIComponent(broker.id)}`}>{broker.name}</Link>
                </td>
                <td>{broker.environment}</td>
                <td className="num">
                  {objectCount} {objectLabel}
                  {namespaceCount > 1 && (
                    <span className="muted">
                      {" "}
                      across {namespaceCount} {namespaceLabelPlural(broker.system_type)}
                    </span>
                  )}
                </td>
                <td className="num">{totalLag === null ? "—" : totalLag}</td>
                <td>
                  <SeverityBadge severity={severity} />
                </td>
              </tr>
            ))}
            {summaries.length === 0 && !loading && (
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
