// Mirrors api/src/api/models.py — the unified shapes the API layer serves.
// Keep these in sync by hand for now; see /docs/architecture.md §3.

export type SystemType = "kafka" | "solace" | "mq";
export type BrokerStatus = "up" | "down" | "degraded";
export type HealthSeverity = "ok" | "warn" | "critical";
export type HealthCategory = "connectivity" | "capacity" | "replication" | "spool";

export interface Broker {
  id: string;
  system_type: SystemType;
  name: string;
  environment: string;
  status: BrokerStatus;
}

export interface Resource {
  id: string;
  broker_id: string;
  system_type: SystemType;
  namespace: string;
  name: string;
  kind: string;
  depth_current: number | null;
  depth_max: number | null;
  consumer_lag: number | null;
  last_updated: string | null;
}

export interface HealthEvent {
  broker_id: string;
  system_type: SystemType;
  severity: HealthSeverity;
  category: HealthCategory;
  message: string;
  timestamp: string;
}

export interface MessageSample {
  resource_id: string;
  message_id: string | null;
  timestamp: string | number | null;
  headers: Record<string, string>;
  body_preview: string;
  size_bytes: number;
}

export interface ConsumerGroupPartition {
  topic: string;
  partition: number;
  log_end_offset: number;
  committed_offset: number | null;
  lag: number | null;
}

export interface ConsumerGroup {
  id: string;
  broker_id: string;
  group_name: string;
  state: string;
  member_count: number;
  partitions: ConsumerGroupPartition[];
}

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8010";

async function getJSON<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`);
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${body}`);
  }
  return resp.json() as Promise<T>;
}

export const api = {
  getBrokers: () => getJSON<Broker[]>("/api/brokers"),
  getResources: () => getJSON<Resource[]>("/api/resources"),
  getResource: (resourceId: string) => getJSON<Resource>(`/api/resources/${encodeURIComponent(resourceId)}`),
  getHealth: () => getJSON<HealthEvent[]>("/api/health"),
  getConsumerGroups: (brokerId?: string) =>
    getJSON<ConsumerGroup[]>(`/api/consumer-groups${brokerId ? `?broker_id=${encodeURIComponent(brokerId)}` : ""}`),
  peekMessages: (resourceId: string, limit = 10) =>
    getJSON<MessageSample[]>(`/api/resources/${encodeURIComponent(resourceId)}/messages?limit=${limit}`),
};
