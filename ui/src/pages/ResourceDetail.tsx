// The "let me actually look at this" screen — deep, single-resource:
// full stats, consumer-group breakdown (Kafka), and message browsing
// (architecture.md §3.1). See the resolved usage-pattern decision in
// /docs/architecture.md §7 — this is the incident-drill-down half of the
// two required surfaces, Overview being the other.

import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api, type ConsumerGroup, type HealthEvent, type MessageSample, type Resource } from "../api";
import { SeverityBadge, SystemTypeBadge } from "../components/Badges";

export default function ResourceDetail() {
  const { resourceId = "" } = useParams<{ resourceId: string }>();

  const [resource, setResource] = useState<Resource | null>(null);
  const [health, setHealth] = useState<HealthEvent[]>([]);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroup[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [messages, setMessages] = useState<MessageSample[] | null>(null);
  const [peekError, setPeekError] = useState<string | null>(null);
  const [peeking, setPeeking] = useState(false);
  const [limit, setLimit] = useState(10);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setMessages(null);
    setPeekError(null);

    (async () => {
      try {
        const r = await api.getResource(resourceId);
        if (cancelled) return;
        setResource(r);

        const [h, groups] = await Promise.all([
          api.getHealth(),
          r.system_type === "kafka" ? api.getConsumerGroups(r.broker_id) : Promise.resolve([]),
        ]);
        if (cancelled) return;
        setHealth(h.filter((e) => e.broker_id === r.broker_id));
        setConsumerGroups(groups.filter((g) => g.partitions.some((p) => p.topic === r.name)));
        setError(null);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [resourceId]);

  async function handlePeek() {
    setPeeking(true);
    setPeekError(null);
    try {
      setMessages(await api.peekMessages(resourceId, limit));
    } catch (e) {
      setPeekError(e instanceof Error ? e.message : String(e));
    } finally {
      setPeeking(false);
    }
  }

  if (loading) return <div className="page">Loading…</div>;
  if (error || !resource) {
    return (
      <div className="page">
        <Link className="breadcrumb" to="/">← Back to overview</Link>
        <div className="error-banner">Couldn't load resource: {error ?? "not found"}</div>
      </div>
    );
  }

  return (
    <div className="page">
      <Link className="breadcrumb" to="/">← Back to overview</Link>
      <h1 className="detail-title">
        <SystemTypeBadge systemType={resource.system_type} /> {resource.name}
      </h1>

      <section className="detail-grid">
        <div>
          <strong>Broker</strong>
          <div>{resource.broker_id}</div>
        </div>
        <div>
          <strong>Namespace</strong>
          <div className="mono">{resource.namespace}</div>
        </div>
        <div>
          <strong>Kind</strong>
          <div>{resource.kind}</div>
        </div>
        <div>
          <strong>Depth</strong>
          <div>{resource.depth_current === null ? "n/a for this system" : `${resource.depth_current}${resource.depth_max ? ` / ${resource.depth_max}` : ""}`}</div>
        </div>
        <div>
          <strong>Consumer lag</strong>
          <div>{resource.consumer_lag === null ? "n/a for this system" : resource.consumer_lag}</div>
        </div>
        <div>
          <strong>Last updated</strong>
          <div>{resource.last_updated ?? "—"}</div>
        </div>
      </section>

      <section>
        <h2>Broker health</h2>
        {health.length === 0 ? (
          <p className="empty-state">No health events reported.</p>
        ) : (
          <ul className="health-list">
            {health.map((h, i) => (
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

      {resource.system_type === "kafka" && consumerGroups.length > 0 && (
        <section>
          <h2>Consumer groups</h2>
          {consumerGroups.map((g) => (
            <div key={g.id} className="consumer-group">
              <h3>
                {g.group_name} <span className="muted">({g.state}, {g.member_count} member(s))</span>
              </h3>
              <div className="table-scroll">
                <table className="resource-table">
                  <thead>
                    <tr>
                      <th>Partition</th>
                      <th>Log end offset</th>
                      <th>Committed offset</th>
                      <th>Lag</th>
                    </tr>
                  </thead>
                  <tbody>
                    {g.partitions
                      .filter((p) => p.topic === resource.name)
                      .map((p) => (
                        <tr key={p.partition}>
                          <td>{p.partition}</td>
                          <td>{p.log_end_offset}</td>
                          <td>{p.committed_offset ?? "—"}</td>
                          <td>{p.lag ?? "no data"}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </section>
      )}

      <section>
        <h2>Messages (non-destructive peek)</h2>
        {resource.system_type === "mq" && (
          <p className="muted">
            IBM MQ's REST Messaging API has no browse cursor — this can only ever show the queue's single oldest message,
            not the next N. See adapters/mq/README.md.
          </p>
        )}
        <div className="peek-controls">
          <label>
            Limit:{" "}
            <input
              type="number"
              min={1}
              max={100}
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              disabled={resource.system_type === "mq"}
            />
          </label>
          <button onClick={handlePeek} disabled={peeking}>
            {peeking ? "Peeking…" : "Peek messages"}
          </button>
        </div>

        {peekError && <div className="error-banner">Peek failed: {peekError}</div>}

        {messages !== null && (
          messages.length === 0 ? (
            <p className="empty-state">No messages on this resource right now.</p>
          ) : (
            <ul className="message-list">
              {messages.map((m, i) => (
                <li key={i} className="message-card">
                  <div className="message-meta">
                    <span className="mono">{m.message_id ?? "no id"}</span>
                    {m.timestamp !== null && <span> · {m.timestamp}</span>}
                    <span> · {m.size_bytes} bytes</span>
                  </div>
                  <pre className="message-body">{m.body_preview}</pre>
                </li>
              ))}
            </ul>
          )
        )}
      </section>
    </div>
  );
}
