// Shared shell for every page: header, the tab bar (Dashboard + one tab
// per connected broker), and the add-broker flow. Tabs are real routes
// (/, /brokers/:id) — open a second real browser tab on a different
// broker's URL to inspect two brokers side by side, which a single-page
// "selected tab" state could never do.

import { useState } from "react";
import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { useMonitoringData } from "../context/MonitoringDataContext";
import { healthForBroker, worstSeverity } from "../healthUtils";
import AddBrokerForm from "./AddBrokerForm";
import ThemeToggle from "./ThemeToggle";
import type { Broker } from "../api";

function tabDotClass(broker: Broker, health: ReturnType<typeof healthForBroker>) {
  if (broker.status !== "up") return "tab-dot status-critical";
  const severity = worstSeverity(health);
  if (severity === "critical") return "tab-dot status-critical";
  if (severity === "warn") return "tab-dot status-warning";
  return "tab-dot status-good";
}

export default function Layout() {
  const { brokers, health, loading, error, reload } = useMonitoringData();
  const [showAddForm, setShowAddForm] = useState(false);
  const navigate = useNavigate();

  const noBrokersConfigured = !loading && brokers.length === 0;

  return (
    <div className="page">
      <header className="page-header">
        <h1>
          <span className="brand-mark" aria-hidden="true">📡</span>
          one-msg-ui
        </h1>
        <div className="header-actions">
          <ThemeToggle />
          <button onClick={() => setShowAddForm((v) => !v)}>{showAddForm ? "Close" : "+ Add broker"}</button>
          <button onClick={reload} disabled={loading}>
            {loading ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>

      {error && <div className="error-banner">Couldn't load data: {error}</div>}

      {showAddForm && (
        <section className="add-broker-panel">
          <AddBrokerForm
            onCreated={(broker) => {
              setShowAddForm(false);
              reload();
              navigate(`/brokers/${encodeURIComponent(broker.id)}`);
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
          {!showAddForm && (
            <nav className="tab-bar">
              <NavLink to="/" end className={({ isActive }) => `tab${isActive ? " tab-active" : ""}`}>
                Dashboard
              </NavLink>
              {brokers.map((b) => (
                <NavLink key={b.id} to={`/brokers/${encodeURIComponent(b.id)}`} className={({ isActive }) => `tab${isActive ? " tab-active" : ""}`}>
                  <span className={tabDotClass(b, healthForBroker(health, b.id))} aria-hidden="true" />
                  {b.name}
                </NavLink>
              ))}
            </nav>
          )}
          <Outlet />
        </>
      )}
    </div>
  );
}
