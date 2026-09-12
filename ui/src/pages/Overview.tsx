// The "what's on fire" screen — broad, shallow, cross-broker. See the
// resolved usage-pattern decision in /docs/architecture.md §7: this and
// the drill-down view (ResourceDetail.tsx) are both first-class, not one
// built at the expense of the other.

import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, type Broker, type HealthEvent, type Resource, type SystemType } from "../api";
import { BrokerStatusBadge, SeverityBadge, SystemTypeBadge } from "../components/Badges";
import AddBrokerForm from "../components/AddBrokerForm";
import StatsSummary from "../components/StatsSummary";
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
  const [showAddForm, setShowAddForm] = useState(false);
  const [removingId, setRemovingId] = useState<string | null>(null);
  const [confirmingRemoveId, setConfirmingRemoveId] = useState<string | null>(null);

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

  async function handleRemoveBroker(brokerId: string) {
    setConfirmingRemoveId(null);
    setRemovingId(brokerId);
    try {
      await api.deleteBroker(brokerId);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRemovingId(null);
    }
  }

  const noBrokersConfigured = !loading && brokers.length === 0;

  return (
    <div className="page">
      <header className="page-header">
        <h1>
          <span className="brand-mark" aria-hidden="true">📡</span>
          one-msg-ui
        </h1>
        <div className="header-actions">
          <button onClick={() => setShowAddForm((v) => !v)}>{showAddForm ? "Close" : "+ Add broker"}</button>
          <button onClick={load} disabled={loading}>
            {loading ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>

      {error && <div className="error-banner">Couldn't load data: {error}</div>}

      {!noBrokersConfigured && <StatsSummary brokers={brokers} resources={resources} health={health} />}

      {showAddForm && (
        <section className="add-broker-panel">
          <AddBrokerForm
            onCreated={() => {
              setShowAddForm(false);
              load();
            }}
            onCancel={() => setShowAddForm(false)}
          />
        </section>
      )}

      {noBrokersConfigured && !showAddForm ? (
        <section className="empty-landing">
          <div className="empty-landing-icon" aria-hidden="true">📡</div>
          <h2>No message brokers configured yet</h2>
          <p className="muted">
            Connect a broker to start monitoring queue depth, consumer lag, and health — or browse messages
            non-destructively. Supported today: Apache Kafka, Solace PubSub+, and IBM MQ.
          </p>
          <button onClick={() => setShowAddForm(true)}>+ Add your first broker</button>
        </section>
      ) : (
        <>
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
                  <div className="broker-card-bottom">
                    <SeverityBadge severity={worstSeverity(brokerHealth)} />
                    {confirmingRemoveId === b.id ? (
                      <span className="remove-confirm">
                        Remove?{" "}
                        <button className="link-button" onClick={() => handleRemoveBroker(b.id)} disabled={removingId === b.id}>
                          {removingId === b.id ? "Removing…" : "Yes"}
                        </button>{" "}
                        <button className="link-button" onClick={() => setConfirmingRemoveId(null)}>
                          No
                        </button>
                      </span>
                    ) : (
                      <button className="link-button" onClick={() => setConfirmingRemoveId(b.id)}>
                        Remove
                      </button>
                    )}
                  </div>
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
                    <td className="num">{r.depth_current === null ? "—" : `${r.depth_current}${r.depth_max ? ` / ${r.depth_max}` : ""}`}</td>
                    <td className="num">{r.consumer_lag === null ? "—" : r.consumer_lag}</td>
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
        </>
      )}
    </div>
  );
}
