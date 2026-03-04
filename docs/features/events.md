# Events

Interloper has a built-in event system for monitoring asset execution, IO operations, and run lifecycle.

## Subscribing to events

```py
import interloper as il

def on_event(event: il.Event):
    print(f"[{event.type.value}] {event.metadata}")

il.subscribe(on_event)

dag.materialize()

il.unsubscribe(on_event)
```

## Event types

| Event | Description |
|-------|-------------|
| `ASSET_STARTED` | Asset materialization started |
| `ASSET_COMPLETED` | Asset materialization completed |
| `ASSET_FAILED` | Asset materialization failed |
| `ASSET_EXEC_STARTED` | Asset function execution started |
| `ASSET_EXEC_COMPLETED` | Asset function execution completed |
| `ASSET_EXEC_FAILED` | Asset function execution failed |
| `IO_READ_STARTED` | IO read started (upstream dependency) |
| `IO_READ_COMPLETED` | IO read completed |
| `IO_READ_FAILED` | IO read failed |
| `IO_WRITE_STARTED` | IO write started |
| `IO_WRITE_COMPLETED` | IO write completed |
| `IO_WRITE_FAILED` | IO write failed |
| `RUN_STARTED` | Runner started a run |
| `RUN_COMPLETED` | Runner completed a run |
| `RUN_FAILED` | Runner run failed |
| `BACKFILL_STARTED` | Backfill started |
| `BACKFILL_COMPLETED` | Backfill completed |
| `BACKFILL_FAILED` | Backfill failed |
| `LOG` | User-emitted log message |


## Event object

```py
event.type        # EventType enum
event.timestamp   # datetime when the event was emitted
event.metadata    # dict with event-specific data
```

Metadata typically includes `asset_key`, `run_id`, `partition_or_window`, and for failures, `error` and `traceback`.

## EventLogger

Inside asset functions, use `context.logger` to emit `LOG` events:

```py
@il.asset
def my_asset(context: il.ExecutionContext):
    context.logger.info("Starting data fetch...")
    context.logger.warning("Rate limited, retrying...")
    context.logger.error("Something went wrong")
    context.logger.debug("Detailed debug info")
    return [{"key": "value"}]
```

These log messages are emitted as `LOG` events and are visible to all subscribers.

## Event forwarding

For distributed runners (Docker, Kubernetes), events can be forwarded from child processes:

```py
il.enable_event_forwarding()   # Forward events via stderr
il.disable_event_forwarding()  # Stop forwarding
```
