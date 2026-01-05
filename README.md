# bv-runtime

Runtime SDK for Bot Velocity automations.

This package provides runtime functionality for automations running in the Bot Velocity platform, including:
- Asset management (get/set text, int, bool, secret, credential assets)
- Queue operations (add/get/update queue items)
- Structured logging (send logs to Orchestrator)

## Installation

The package is automatically added to your project dependencies when you run `bv init`. It will be installed in your project's virtual environment and locked in `requirements.lock` when you publish.

## Usage

**Note:** The runtime SDK checks for the `BV_SDK_RUN` environment variable to prevent accidental usage outside of a controlled runner context. If running scripts manually (e.g. `python main.py`), you must set `BV_SDK_RUN=1`.

```python
from bv.runtime import assets, queues, log_message, LogLevel

# Get assets
text_value = assets.get_asset("my_text_asset")
int_value = assets.get_asset("my_int_asset")
secret_value = assets.get_secret("my_secret_asset")
credential = assets.get_credential("my_credential_asset")

# Set assets
assets.set_asset("my_text_asset", "new value")
assets.set_secret("my_secret_asset", encrypted_value)
assets.set_credential("my_credential_asset", "username", encrypted_password)

# Queue operations
item_id = queues.add_queue_item("my_queue", {"key": "value"})
item = queues.get_queue_item("my_queue")
queues.set_queue_item_status(item_id, "completed", result={"status": "ok"})

# Logging
log_message("Processing started", LogLevel.INFO)
log_message("Warning: retry needed", LogLevel.WARN)
log_message("Error occurred", LogLevel.ERROR)
```

## Authentication

The runtime supports two authentication modes:

1. **Runner mode** (environment-based): Uses `BV_ORCHESTRATOR_URL` and `BV_ROBOT_TOKEN` environment variables
2. **Development mode** (file-based): Uses `~/.bv/auth.json` file created by `bv auth login`

The runtime automatically detects which mode to use based on available environment variables.

