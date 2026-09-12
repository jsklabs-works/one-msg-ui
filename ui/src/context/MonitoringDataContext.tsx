// One shared poll for the whole app instead of each page fetching (and
// re-fetching on every tab switch) independently. Dashboard, per-broker
// tabs, and the resource drill-down all read from this same snapshot —
// switching tabs is instant and never triggers a fresh network round trip.

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { api, type Broker, type ConsumerGroup, type HealthEvent, type Resource } from "../api";

const REFRESH_INTERVAL_MS = 15_000; // matches the 15-60s polling cadence architecture.md §4 recommends

interface MonitoringData {
  brokers: Broker[];
  resources: Resource[];
  health: HealthEvent[];
  consumerGroups: ConsumerGroup[];
  loading: boolean;
  error: string | null;
  reload: () => Promise<void>;
}

const MonitoringDataContext = createContext<MonitoringData | null>(null);

export function MonitoringDataProvider({ children }: { children: ReactNode }) {
  const [brokers, setBrokers] = useState<Broker[]>([]);
  const [resources, setResources] = useState<Resource[]>([]);
  const [health, setHealth] = useState<HealthEvent[]>([]);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroup[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function reload() {
    try {
      const [b, r, h, g] = await Promise.all([
        api.getBrokers(),
        api.getResources(),
        api.getHealth(),
        api.getConsumerGroups(),
      ]);
      setBrokers(b);
      setResources(r);
      setHealth(h);
      setConsumerGroups(g);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    reload();
    const id = setInterval(reload, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, []);

  return (
    <MonitoringDataContext.Provider value={{ brokers, resources, health, consumerGroups, loading, error, reload }}>
      {children}
    </MonitoringDataContext.Provider>
  );
}

export function useMonitoringData(): MonitoringData {
  const ctx = useContext(MonitoringDataContext);
  if (!ctx) throw new Error("useMonitoringData must be used within a MonitoringDataProvider");
  return ctx;
}
