// Bulk broker import — upload a JSON file shaped like api/config/brokers.json
// (a top-level "brokers" array) instead of filling the add-broker form once
// per broker. Each entry gets exactly the same validation/duplicate-name/
// duplicate-connection/connection-test checks as adding one manually (see
// api/src/api/main.py's _add_one_broker); one bad entry doesn't block the
// rest, so the result is a per-broker status list, not a single pass/fail.

import { useState } from "react";
import { api, type BrokerImportEntry, type BrokerImportResult } from "../api";

const STATUS_LABEL: Record<BrokerImportResult["status"], string> = {
  added: "Added",
  duplicate_name: "Skipped — name already used",
  duplicate_connection: "Skipped — duplicate connection",
  invalid: "Skipped — invalid config",
  connection_failed: "Skipped — couldn't connect",
};

function isBrokerImportEntry(value: unknown): value is BrokerImportEntry {
  if (!value || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.type === "string" &&
    typeof v.name === "string" &&
    typeof v.environment === "string" &&
    typeof v.config === "object" &&
    v.config !== null
  );
}

export default function ImportBrokersForm({ onImported, onCancel }: { onImported: () => void; onCancel: () => void }) {
  const [fileName, setFileName] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [entries, setEntries] = useState<BrokerImportEntry[] | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [results, setResults] = useState<BrokerImportResult[] | null>(null);

  async function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    setFileName(file.name);
    setParseError(null);
    setEntries(null);
    setResults(null);
    setSubmitError(null);
    try {
      const data = JSON.parse(await file.text());
      const list = Array.isArray(data) ? data : data.brokers;
      if (!Array.isArray(list)) {
        throw new Error('expected a top-level "brokers" array — the same shape as api/config/brokers.json');
      }
      const badIndex = list.findIndex((item) => !isBrokerImportEntry(item));
      if (badIndex !== -1) {
        throw new Error(`entry ${badIndex + 1} is missing one of type/name/environment/config`);
      }
      setEntries(list as BrokerImportEntry[]);
    } catch (err) {
      setParseError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleImport() {
    if (!entries) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      const importResults = await api.importBrokers(entries);
      setResults(importResults);
      if (importResults.some((r) => r.status === "added")) onImported();
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  const addedCount = results?.filter((r) => r.status === "added").length ?? 0;

  return (
    <div className="import-brokers-form">
      <p className="muted">
        Upload a JSON file shaped like <code>api/config/brokers.json</code> — a top-level <code>brokers</code> array of{" "}
        <code>{"{type, name, environment, config}"}</code> — to add several brokers at once instead of one at a time.
        Each entry gets the same duplicate and connection checks as adding one manually.
      </p>

      <div className="form-row">
        <input type="file" accept="application/json,.json" onChange={handleFileChange} disabled={submitting} />
      </div>

      {parseError && (
        <div className="error-banner">
          Couldn't read {fileName}: {parseError}
        </div>
      )}
      {submitError && <div className="error-banner">{submitError}</div>}

      {entries && !results && (
        <p className="muted">
          {fileName}: {entries.length} broker{entries.length === 1 ? "" : "s"} found.
        </p>
      )}

      {results && (
        <>
          <p className="muted">
            {addedCount} of {results.length} broker{results.length === 1 ? "" : "s"} added.
          </p>
          <ul className="import-results">
            {results.map((r, i) => (
              <li key={i} className={`import-result import-result-${r.status === "added" ? "ok" : "skip"}`}>
                <span className={`badge badge-system-${r.type}`}>{r.type}</span>
                <strong>{r.name}</strong>
                <span className="import-result-status">{STATUS_LABEL[r.status]}</span>
                {r.status !== "added" && <span className="muted import-result-detail">{r.detail}</span>}
              </li>
            ))}
          </ul>
        </>
      )}

      <div className="form-actions">
        <button type="button" onClick={onCancel} disabled={submitting}>
          {results ? "Close" : "Cancel"}
        </button>
        {!results && (
          <button type="button" onClick={handleImport} disabled={!entries || submitting}>
            {submitting ? "Importing…" : `Import${entries ? ` ${entries.length} broker${entries.length === 1 ? "" : "s"}` : ""}`}
          </button>
        )}
      </div>
    </div>
  );
}
