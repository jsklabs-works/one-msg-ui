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
  // The actual destination the message arrived on. Kafka: always the topic
  // being browsed. Solace: can differ from the queue being browsed if it
  // has a topic subscription. MQ: always null — no topic concept.
  topic: string | null;
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

// Drives the "add broker" form dynamically — see api/src/api/config.py's
// FIELD_SPECS, the single source of truth this mirrors at request time
// (not hand-copied here, so a new adapter type needs no UI code change).
export interface FieldSpec {
  name: string;
  label: string;
  type: "text" | "password" | "checkbox";
  required: boolean;
  default: string | null;
  placeholder: string | null;
}

export interface SystemTypeInfo {
  type: SystemType;
  label: string;
  fields: FieldSpec[];
}

export interface CreateBrokerRequest {
  type: SystemType;
  name: string;
  environment: string;
  config: Record<string, string>;
}

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8010";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`, init);
  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      const body = await resp.json();
      detail = body.detail ?? JSON.stringify(body);
    } catch {
      // response wasn't JSON — fall back to statusText
    }
    throw new Error(detail);
  }
  if (resp.status === 204) return undefined as T;
  return resp.json() as Promise<T>;
}

const getJSON = <T,>(path: string) => request<T>(path);

export const api = {
  getBrokers: () => getJSON<Broker[]>("/api/brokers"),
  getResources: () => getJSON<Resource[]>("/api/resources"),
  getResource: (resourceId: string) => getJSON<Resource>(`/api/resources/${encodeURIComponent(resourceId)}`),
  getHealth: () => getJSON<HealthEvent[]>("/api/health"),
  getConsumerGroups: (brokerId?: string) =>
    getJSON<ConsumerGroup[]>(`/api/consumer-groups${brokerId ? `?broker_id=${encodeURIComponent(brokerId)}` : ""}`),
  peekMessages: (resourceId: string, limit = 10) =>
    getJSON<MessageSample[]>(`/api/resources/${encodeURIComponent(resourceId)}/messages?limit=${limit}`),
  // A password has no business in a URL query string, so retrying with
  // different credentials goes over POST instead of the GET above.
  // Nothing here is saved to the broker config — just this one retry.
  peekMessagesWithCredentials: (resourceId: string, limit: number, username: string, password: string) =>
    request<MessageSample[]>(`/api/resources/${encodeURIComponent(resourceId)}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ limit, username, password }),
    }),
  getSystemTypes: () => getJSON<SystemTypeInfo[]>("/api/system-types"),
  createBroker: (req: CreateBrokerRequest) =>
    request<Broker>("/api/brokers", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    }),
  deleteBroker: (brokerId: string) =>
    request<void>(`/api/brokers/${encodeURIComponent(brokerId)}`, { method: "DELETE" }),
};
