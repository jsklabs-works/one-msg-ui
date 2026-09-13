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
import ImportBrokersForm from "./ImportBrokersForm";
import ThemeToggle from "./ThemeToggle";
import { api, type Broker } from "../api";

// The other half of the import round trip — downloads exactly what
// POST /api/brokers/import accepts, so exporting from one instance and
// importing into another (or just backing up the current inventory)
// needs no reshaping. Includes each broker's full config, credentials
// included — same plaintext-in-JSON shape config/brokers.json already
// is on disk (see api/src/api/config.py's module docstring), not new
// exposure, but worth a person knowing before they email the file around.
async function downloadBrokersExport() {
  const { brokers } = await api.exportBrokers();
  const blob = new Blob([JSON.stringify({ brokers }, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "brokers.json";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

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
  const [showImportForm, setShowImportForm] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const navigate = useNavigate();

  const noBrokersConfigured = !loading && brokers.length === 0;

  async function handleExport() {
    setExporting(true);
    setExportError(null);
    try {
      await downloadBrokersExport();
    } catch (err) {
      setExportError(err instanceof Error ? err.message : String(err));
    } finally {
      setExporting(false);
    }
  }

  return (
    <div className="page">
      <header className="page-header">
        <h1>
          <span className="brand-mark" aria-hidden="true">📡</span>
          one-msg-ui
        </h1>
        <div className="header-actions">
          <ThemeToggle />
          <button
            onClick={() => {
              setShowAddForm((v) => !v);
              setShowImportForm(false);
            }}
          >
            {showAddForm ? "Close" : "+ Add broker"}
          </button>
          <button
            onClick={() => {
              setShowImportForm((v) => !v);
              setShowAddForm(false);
            }}
          >
            {showImportForm ? "Close" : "Import JSON"}
          </button>
          <button
            onClick={handleExport}
            disabled={exporting || noBrokersConfigured}
            title="Download every configured broker as JSON, including credentials in plain text"
          >
            {exporting ? "Exporting…" : "Export JSON"}
          </button>
          <button onClick={reload} disabled={loading}>
            {loading ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>

      {error && <div className="error-banner">Couldn't load data: {error}</div>}
      {exportError && <div className="error-banner">Couldn't export brokers: {exportError}</div>}

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

      {showImportForm && (
        <section className="add-broker-panel">
          <ImportBrokersForm onImported={reload} onCancel={() => setShowImportForm(false)} />
        </section>
      )}

      {noBrokersConfigured && !showAddForm && !showImportForm ? (
        <section className="empty-landing">
          <div className="empty-landing-icon" aria-hidden="true">📡</div>
          <h2>No message brokers configured yet</h2>
          <p className="muted">
            Connect a broker to start monitoring queue depth, consumer lag, and health — or browse messages
            non-destructively. Supported today: Apache Kafka, Solace PubSub+, and IBM MQ.
          </p>
          <div className="empty-landing-actions">
            <button onClick={() => setShowAddForm(true)}>+ Add your first broker</button>
            <button onClick={() => setShowImportForm(true)}>Import from JSON</button>
          </div>
        </section>
      ) : (
        <>
          {!showAddForm && !showImportForm && (
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
