# Change Data Capture
This project demonstrates Change Data Capture (CDC) using Postgres, Debezium, and Redpanda.

## Components

- **Postgres**: Configured with `wal_level=logical` to enable CDC capabilities
- **Debezium**: CDC connector that monitors Postgres WAL changes
- **Redpanda**: Kafka-compatible event streaming platform
- **Redpanda Console**: Web UI for monitoring topics and messages

## Key Configuration Notes

### PostgreSQL
- WAL (Write-Ahead Logging) is set to logical level via `postgres -c wal_level=logical`
- Database name: `pandashop`
- Port: 5432

### Debezium
- Runs on port 8083
- Configured to connect to Redpanda broker at `redpanda:9092`
- Monitors Postgres for changes
- Creates topics with prefix `dbz`

### Redpanda
- Kafka-compatible API available at:
  - Internal: `redpanda:9092`
  - External: `localhost:19092`
- Console UI accessible at port 8080

## Getting Started

1. Start the services
   ```bash
   docker compose up -d
   ```
2. Run the script for dbz postgres connector 
   ```bash
   bash create-debezium-postgres-connector.sh
   ```
