// One tab per connected broker — this is where "Inspect" lives now
// (removed from the Dashboard tab per request), so drilling into a
// resource always happens from the broker you're already looking at.

import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api, type Resource } from "../api";
import { BrokerStatusBadge, SeverityBadge, SystemTypeBadge } from "../components/Badges";
import { useMonitoringData } from "../context/MonitoringDataContext";
import { NAMESPACE_LABELS, kindLabel } from "../labels";

function groupByNamespace(resources: Resource[]): [string, Resource[]][] {
  const groups = new Map<string, Resource[]>();
  for (const r of resources) {
    const group = groups.get(r.namespace);
    if (group) group.push(r);
    else groups.set(r.namespace, [r]);
  }
  return Array.from(groups.entries());
}

export default function BrokerView() {
  const { brokerId = "" } = useParams<{ brokerId: string }>();
  const { brokers, resources, health, loading, reload } = useMonitoringData();
  const navigate = useNavigate();
  const [confirmingRemove, setConfirmingRemove] = useState(false);
  const [removing, setRemoving] = useState(false);
  const [removeError, setRemoveError] = useState<string | null>(null);

  const broker = brokers.find((b) => b.id === brokerId);
  const brokerResources = resources.filter((r) => r.broker_id === brokerId);
  const brokerHealth = health.filter((h) => h.broker_id === brokerId);
  const namespaceGroups = useMemo(() => groupByNamespace(brokerResources), [brokerResources]);
  const namespaceLabel = broker ? NAMESPACE_LABELS[broker.system_type] : "Namespace";
  // Every resource on one broker shares one kind (a Kafka broker's are
  // all topics, an MQ/Solace one's all queues) — see labels.ts for why
  // this reads off the actual data instead of the system type.
  const resourceKind = brokerResources[0]?.kind;
  const kindSingular = resourceKind ? kindLabel(resourceKind) : "Resource";
  const kindPlural = resourceKind ? kindLabel(resourceKind, true) : "Resources";

  async function handleRemove() {
    setRemoving(true);
    setRemoveError(null);
    try {
      await api.deleteBroker(brokerId);
      await reload();
      navigate("/");
    } catch (e) {
      setRemoveError(e instanceof Error ? e.message : String(e));
      setRemoving(false);
    }
  }

  if (!loading && !broker) {
    return (
      <div>
        <p className="error-banner">No such broker: {brokerId}. It may have just been removed.</p>
        <Link to="/">← Back to dashboard</Link>
      </div>
    );
  }
  if (!broker) return <p className="muted">Loading…</p>;

  return (
    <div>
      <div className="detail-grid">
        <div>
          <strong>System</strong>
          <SystemTypeBadge systemType={broker.system_type} />
        </div>
        <div>
          <strong>Status</strong>
          <BrokerStatusBadge status={broker.status} />
        </div>
        <div>
          <strong>Environment</strong>
          <div>{broker.environment}</div>
        </div>
      </div>

      <section>
        <h2>Health</h2>
        {brokerHealth.length === 0 ? (
          <p className="empty-state">No health events reported.</p>
        ) : (
          <ul className="health-list">
            {brokerHealth.map((h, i) => (
              <li key={i}>
                <div className="health-list-head">
                  <SeverityBadge severity={h.severity} />
                  <strong>{h.category}</strong>
                </div>
                <div className="health-list-message">{h.message}</div>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section>
        <h2>{kindPlural}</h2>
        {brokerResources.length === 0 && !loading ? (
          <p className="empty-state">No {kindPlural.toLowerCase()} on this broker.</p>
        ) : (
          namespaceGroups.map(([namespace, groupResources]) => (
            <div key={namespace} className="namespace-group">
              <h3>
                {namespaceLabel}: <span className="mono">{namespace}</span>
              </h3>
              <div className="table-scroll">
                <table className="resource-table">
                  <thead>
                    <tr>
                      <th>{kindSingular}</th>
                      <th>Depth</th>
                      <th>Consumer lag</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {groupResources.map((r) => (
                      <tr key={r.id}>
                        <td className="mono">{r.name}</td>
                        <td className="num">{r.depth_current === null ? "—" : `${r.depth_current}${r.depth_max ? ` / ${r.depth_max}` : ""}`}</td>
                        <td className="num">{r.consumer_lag === null ? "—" : r.consumer_lag}</td>
                        <td>
                          <Link to={`/resources/${encodeURIComponent(r.id)}`}>Inspect →</Link>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))
        )}
      </section>

      <section>
        <h2>Danger zone</h2>
        {removeError && <div className="error-banner">{removeError}</div>}
        {confirmingRemove ? (
          <span className="remove-confirm">
            Remove {broker.name}? This only forgets the connection, it doesn't touch the broker itself.{" "}
            <button className="link-button" onClick={handleRemove} disabled={removing}>
              {removing ? "Removing…" : "Yes, remove"}
            </button>{" "}
            <button className="link-button" onClick={() => setConfirmingRemove(false)}>
              Cancel
            </button>
          </span>
        ) : (
          <button className="button-danger" onClick={() => setConfirmingRemove(true)}>
            Remove this broker
          </button>
        )}
      </section>
    </div>
  );
}
