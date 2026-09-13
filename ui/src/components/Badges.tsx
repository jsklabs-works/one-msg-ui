import type { BrokerStatus, HealthSeverity, SystemType } from "../api";

const SYSTEM_LABELS: Record<SystemType, string> = {
  kafka: "Kafka",
  solace: "Solace",
  mq: "IBM MQ",
  rabbitmq: "RabbitMQ",
  activemq: "ActiveMQ",
};

export function SystemTypeBadge({ systemType }: { systemType: SystemType }) {
  return <span className={`badge badge-system badge-system-${systemType}`}>{SYSTEM_LABELS[systemType]}</span>;
}

export function SeverityBadge({ severity }: { severity: HealthSeverity }) {
  return <span className={`badge badge-severity badge-severity-${severity}`}>{severity}</span>;
}

export function BrokerStatusBadge({ status }: { status: BrokerStatus }) {
  return <span className={`badge badge-status badge-status-${status}`}>{status}</span>;
}
