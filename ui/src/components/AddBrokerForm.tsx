// The dropdown-of-supported-tools flow: pick a system type, get that
// type's real connection fields (from GET /api/system-types — driven by
// api/src/api/config.py's FIELD_SPECS, not hardcoded here), submit, and
// the API actually tries to connect before saving anything.

import { useEffect, useState } from "react";
import { api, type Broker, type SystemType, type SystemTypeInfo } from "../api";

const ENVIRONMENT_PRESETS = ["dev", "uat", "prod"];

export default function AddBrokerForm({
  onCreated,
  onCancel,
}: {
  onCreated: (broker: Broker) => void;
  onCancel: () => void;
}) {
  const [systemTypes, setSystemTypes] = useState<SystemTypeInfo[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [selectedType, setSelectedType] = useState<SystemType | "">("");
  const [name, setName] = useState("");
  const [environment, setEnvironment] = useState("dev");
  const [configValues, setConfigValues] = useState<Record<string, string>>({});

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  useEffect(() => {
    api
      .getSystemTypes()
      .then(setSystemTypes)
      .catch((e) => setLoadError(e instanceof Error ? e.message : String(e)));
  }, []);

  const selectedInfo = systemTypes?.find((t) => t.type === selectedType) ?? null;

  function handleTypeChange(type: SystemType) {
    setSelectedType(type);
    const info = systemTypes?.find((t) => t.type === type);
    const defaults: Record<string, string> = {};
    for (const field of info?.fields ?? []) {
      if (field.default) defaults[field.name] = field.default;
    }
    setConfigValues(defaults);
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!selectedType) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      const broker = await api.createBroker({ type: selectedType, name, environment, config: configValues });
      onCreated(broker);
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  if (loadError) return <div className="error-banner">Couldn't load supported systems: {loadError}</div>;
  if (!systemTypes) return <p className="muted">Loading supported systems…</p>;

  return (
    <form className="add-broker-form" onSubmit={handleSubmit}>
      <div className="form-row">
        <label>
          Messaging system
          <select value={selectedType} onChange={(e) => handleTypeChange(e.target.value as SystemType)} required>
            <option value="" disabled>
              Select a system…
            </option>
            {systemTypes.map((t) => (
              <option key={t.type} value={t.type}>
                {t.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      {selectedInfo && (
        <>
          <div className="form-row">
            <label>
              Name
              <input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Prod Kafka Cluster" required />
            </label>
          </div>
          <div className="form-row">
            <label>
              Environment
              <input
                value={environment}
                onChange={(e) => setEnvironment(e.target.value)}
                list="environment-presets"
                required
              />
              <datalist id="environment-presets">
                {ENVIRONMENT_PRESETS.map((env) => (
                  <option key={env} value={env} />
                ))}
              </datalist>
            </label>
          </div>

          <fieldset>
            <legend>{selectedInfo.label} connection</legend>
            {selectedInfo.fields.map((field) =>
              field.type === "checkbox" ? (
                <div className="form-row form-row-checkbox" key={field.name}>
                  <label>
                    <input
                      type="checkbox"
                      checked={configValues[field.name] === "true"}
                      onChange={(e) => setConfigValues((prev) => ({ ...prev, [field.name]: e.target.checked ? "true" : "false" }))}
                    />
                    {field.label}
                  </label>
                </div>
              ) : (
                <div className="form-row" key={field.name}>
                  <label>
                    {field.label}
                    {!field.required && <span className="muted"> (optional)</span>}
                    <input
                      type={field.type}
                      value={configValues[field.name] ?? ""}
                      onChange={(e) => setConfigValues((prev) => ({ ...prev, [field.name]: e.target.value }))}
                      placeholder={field.placeholder ?? undefined}
                      required={field.required}
                    />
                  </label>
                </div>
              ),
            )}
          </fieldset>
        </>
      )}

      {submitError && <div className="error-banner">{submitError}</div>}

      <div className="form-actions">
        <button type="button" onClick={onCancel} disabled={submitting}>
          Cancel
        </button>
        <button type="submit" disabled={!selectedType || submitting}>
          {submitting ? "Testing connection…" : "Test connection & add"}
        </button>
      </div>
    </form>
  );
}
