# Interloper DBOS Integration

This package provides DBOS integration for Interloper, enabling durable workflow execution with automatic recovery from failures.

## Features

- **Durable Execution**: Assets are executed as DBOS steps within durable workflows
- **Automatic Recovery**: Failed workflows can be resumed from the last successful step
- **Sequential Execution**: Assets execute in dependency order with DBOS checkpointing
- **Workflow Management**: Support for custom workflow IDs and automatic ID generation

## Installation

```bash
pip install interloper-dbos
```

## Prerequisites

- PostgreSQL database for DBOS state storage
- Python 3.10+

## Quick Start

```python
import interloper as il
import datetime as dt
from interloper_dbos import DBOSConfig, DBOSRunner

# Define your pipeline
@il.asset(io=il.FileIO("data/"))
def my_asset(context: il.Context) -> pd.DataFrame:
    return pd.DataFrame({"value": [1, 2, 3]})

# Create DAG
dag = il.DAG(my_asset)

# Configure DBOS
dbos_config = DBOSConfig(
    host="localhost",
    database="interloper_dbos",
    user="postgres",
    password="your_password"
)

# Create runner
runner = DBOSRunner()

# Execute with durability
result = runner.run(dag)
```

## Configuration

### DBOSConfig

The `DBOSConfig` class handles database connection and DBOS settings:

```python
from interloper_dbos import DBOSConfig

config = DBOSConfig(
    host="localhost",           # Database host
    port=5432,                 # Database port (default: 5432)
    database="interloper_dbos", # Database name
    user="postgres",           # Database user
    password="your_password",   # Database password
    max_connections=10,        # Max DB connections (default: 10)
    connection_timeout=30,     # Connection timeout in seconds (default: 30)
    step_timeout=3600,        # Step timeout in seconds (default: 3600)
    max_retries=3,            # Max step retries (default: 3)
)
```

### DBOSRunner

The `DBOSRunner` extends Interloper's base runner with DBOS durability:

```python
from interloper_dbos import DBOSRunner

runner = DBOSRunner(
    concurrency=10  # The concurrency of the DBOS queue
)
```

## Workflow Recovery

DBOS enables automatic recovery from failures. You can resume workflows using the same workflow ID:

```python
# First execution
result = runner.run(dag, workflow_id="my_pipeline_20250101")

# If it fails, retry with the same workflow ID to resume
result = runner.run(dag, workflow_id="my_pipeline_20250101")
```

### Automatic Workflow ID Generation

If you don't specify a workflow ID, one will be generated automatically:

- For partitioned assets: `{prefix}_{partition}_{timestamp}`
- For partition windows: `{prefix}_{start}_{end}_{timestamp}`
- For non-partitioned: `{prefix}_{timestamp}_{uuid}`

## Database Setup

You need a PostgreSQL database for DBOS to store workflow state:

```sql
-- Create database
CREATE DATABASE interloper_dbos;

-- Create user (optional)
CREATE USER interloper_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE interloper_dbos TO interloper_user;
```

## Examples

See the `examples/` directory for complete examples:

- `simple_pipeline.py`: Basic pipeline with recovery demonstration
- More examples coming soon...

## How It Works

1. **Workflow Creation**: Each `run()` call creates a DBOS workflow
2. **Asset Steps**: Each asset becomes a DBOS step within the workflow
3. **Sequential Execution**: Assets execute in topological order
4. **Checkpointing**: DBOS automatically checkpoints each completed step
5. **Recovery**: Failed workflows resume from the last successful step

## Comparison with Other Runners

| Feature | SerialRunner | MultiThreadRunner | DBOSRunner |
|---------|---------------|-------------------|------------|
| Parallelism | No | Yes | No (planned) |
| Durability | No | No | Yes |
| Recovery | No | No | Yes |
| State Storage | None | None | PostgreSQL |

## Limitations

- Currently supports sequential execution only (parallel execution planned)
- Requires PostgreSQL database setup
- Workflow state is stored in the database (consider cleanup for old workflows)

## Contributing

Contributions are welcome! Please see the main Interloper repository for contribution guidelines.

## License

This package is part of the Interloper project. See the main repository for license information.
