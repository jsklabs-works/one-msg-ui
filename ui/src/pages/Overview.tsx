// The "what's on fire" screen — broad, shallow, cross-broker. See the
// resolved usage-pattern decision in /docs/architecture.md §7: this and
// the drill-down view (ResourceDetail.tsx) are both first-class, not one
// built at the expense of the other.

import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, type Broker, type HealthEvent, type Resource, type SystemType } from "../api";
import { BrokerStatusBadge, SeverityBadge, SystemTypeBadge } from "../components/Badges";
import { healthForBroker, worstSeverity } from "../healthUtils";

const REFRESH_INTERVAL_MS = 15_000; // matches the 15-60s polling cadence architecture.md §4 recommends

export default function Overview() {
  const [brokers, setBrokers] = useState<Broker[]>([]);
  const [resources, setResources] = useState<Resource[]>([]);
  const [health, setHealth] = useState<HealthEvent[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [environmentFilter, setEnvironmentFilter] = useState<string>("all");
  const [systemTypeFilter, setSystemTypeFilter] = useState<SystemType | "all">("all");

  async function load() {
    try {
      const [b, r, h] = await Promise.all([api.getBrokers(), api.getResources(), api.getHealth()]);
      setBrokers(b);
      setResources(r);
      setHealth(h);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, []);

  const environments = useMemo(() => Array.from(new Set(brokers.map((b) => b.environment))).sort(), [brokers]);
  const brokerById = useMemo(() => Object.fromEntries(brokers.map((b) => [b.id, b])), [brokers]);

  const filteredResources = resources.filter((r) => {
    const broker = brokerById[r.broker_id];
    if (systemTypeFilter !== "all" && r.system_type !== systemTypeFilter) return false;
    if (environmentFilter !== "all" && broker?.environment !== environmentFilter) return false;
    return true;
  });

  return (
    <div className="page">
      <header className="page-header">
        <h1>one-msg-ui</h1>
        <button onClick={load} disabled={loading}>
          {loading ? "Refreshing…" : "Refresh"}
        </button>
      </header>

      {error && <div className="error-banner">Couldn't load data: {error}</div>}

      <section className="broker-strip">
        {brokers.map((b) => {
          const brokerHealth = healthForBroker(health, b.id);
          return (
            <div key={b.id} className="broker-card">
              <div className="broker-card-top">
                <SystemTypeBadge systemType={b.system_type} />
                <BrokerStatusBadge status={b.status} />
              </div>
              <div className="broker-card-name">{b.name}</div>
              <div className="broker-card-env">{b.environment}</div>
              <SeverityBadge severity={worstSeverity(brokerHealth)} />
            </div>
          );
        })}
      </section>

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
              <th>Resource</th>
              <th>Depth</th>
              <th>Consumer lag</th>
              <th></th>
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
                <td>{r.depth_current === null ? "—" : `${r.depth_current}${r.depth_max ? ` / ${r.depth_max}` : ""}`}</td>
                <td>{r.consumer_lag === null ? "—" : r.consumer_lag}</td>
                <td>
                  <Link to={`/resources/${encodeURIComponent(r.id)}`}>Inspect →</Link>
                </td>
              </tr>
            ))}
            {filteredResources.length === 0 && !loading && (
              <tr>
                <td colSpan={7} className="empty-state">
                  No resources match this filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
