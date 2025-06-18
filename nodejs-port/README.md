# Exoquic Agent - Node.js Port

A Node.js port of the Exoquic PostgreSQL replication agent. This agent captures database changes using PostgreSQL's logical replication and forwards them to the Exoquic platform.

## Features

- **PostgreSQL Change Data Capture**: Captures INSERT, UPDATE, DELETE operations using logical replication
- **Transaction Consistency**: Groups operations by transaction to maintain ACID properties
- **Event Streaming**: Converts database changes to JSON and forwards to API endpoint
- **Automatic Setup**: Configures replication slots, publications, and WAL settings
- **Graceful Shutdown**: Handles in-flight transactions during shutdown
- **Docker Support**: Includes Docker and Docker Compose configurations

## Requirements

- Node.js 18+ (ES modules support)
- PostgreSQL 12+ with logical replication enabled
- Database user with replication privileges

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd nodejs-port
```

2. Install dependencies:
```bash
npm install
```

3. Copy environment configuration:
```bash
cp .env.example .env
```

4. Configure your environment variables in `.env`:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

EXOQUIC_ENDPOINT=https://api.exoquic.com
EXOQUIC_API_KEY=your-api-key

REPLICATION_SLOT_NAME=realtime_slot
PUBLICATION_NAME=realtime_publication

LOG_LEVEL=info
```

## Usage

### Development Mode

```bash
npm run dev
```

### Production Mode

```bash
npm start
```

### Docker

Build and run with Docker:
```bash
docker build -t exoquic-agent-nodejs .
docker run --env-file .env exoquic-agent-nodejs
```

### Docker Compose

For testing with a PostgreSQL instance:
```bash
docker-compose up
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_DATABASE` | Database name | `postgres` |
| `POSTGRES_USER` | Database user | `postgres` |
| `POSTGRES_PASSWORD` | Database password | `password` |
| `EXOQUIC_ENDPOINT` | Exoquic API endpoint | `https://api.exoquic.com` |
| `EXOQUIC_API_KEY` | Exoquic API key | Required |
| `REPLICATION_SLOT_NAME` | Replication slot name | `realtime_slot` |
| `PUBLICATION_NAME` | Publication name | `realtime_publication` |
| `LOG_LEVEL` | Log level | `info` |

### PostgreSQL Setup

The agent will automatically configure PostgreSQL with the required settings:

1. **WAL Level**: Sets `wal_level = logical`
2. **Replication Slots**: Sets `max_replication_slots >= 5`
3. **WAL Senders**: Sets `max_wal_senders >= 5`
4. **Publication**: Creates publication for all tables
5. **Replication Slot**: Creates logical replication slot
6. **Replica Identity**: Sets `REPLICA IDENTITY FULL` for tables without primary keys

**Note**: PostgreSQL restart may be required after parameter changes.

### User Permissions

The database user must have:
- `REPLICATION` privilege
- `USAGE` privilege on the target schema
- `CREATE` privilege for creating publications and replication slots

```sql
-- Grant replication privileges
ALTER USER your_user REPLICATION;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO your_user;

-- Grant create privileges
GRANT CREATE ON DATABASE your_database TO your_user;
```

## Event Format

Events are sent to the Exoquic platform in the following JSON format:

```json
{
  "type": "created|updated|deleted",
  "data": {
    "column1": "value1",
    "column2": "value2",
    ...
  }
}
```

## Architecture

### Core Components

- **PostgresReplication**: Main replication service
- **TransactionBuffer**: Groups events by transaction
- **JsonEventConverter**: Converts database changes to JSON
- **Config**: Configuration management

### pgoutput Protocol

- **PgOutputConfig**: Configuration for pgoutput plugin
- **ReplicationMessage**: Represents replication events
- **Operation**: Enumeration of operation types

## Differences from Java Version

### Advantages

- **Lighter footprint**: No JVM overhead
- **Simpler deployment**: Single binary with dependencies
- **Native async**: Built-in Promise/async-await support
- **JSON handling**: Native JSON support

### Limitations

- **Protocol parsing**: Uses `pg-logical-replication` library instead of custom implementation
- **Type system**: JavaScript's dynamic typing vs Java's static typing
- **Error handling**: Different error handling patterns

## Troubleshooting

### Common Issues

1. **Permission denied**: Ensure user has replication privileges
2. **WAL level not logical**: Restart PostgreSQL after configuration changes
3. **Connection refused**: Check PostgreSQL connection settings
4. **API errors**: Verify Exoquic endpoint and API key

### Debugging

Enable debug logging:
```bash
LOG_LEVEL=debug npm start
```

### Health Checks

The application includes graceful shutdown handling for:
- SIGINT (Ctrl+C)
- SIGTERM (Docker stop)

## Testing

Run tests:
```bash
npm test
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

Same as the original Java version.