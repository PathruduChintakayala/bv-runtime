from __future__ import annotations

from typing import Any

from bv.runtime._guard import require_bv_run


def add(
    queue_name: str,
    *,
    content: Any,
    reference: str | None = None,
    priority: int = 0,
) -> "QueueItem":
    """Enqueue an item and return a typed QueueItem.

    Backend contract is unchanged; we only wrap the response into an immutable QueueItem.
    """
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    from bv.runtime.queue_item import QueueItem

    client = OrchestratorClient()
    body = {
        "queue_name": queue_name,
        "payload": content,  # backend expects 'payload'
        "reference": reference,
        "priority": priority,
    }
    resp = client.request("POST", "/api/queue-items/add", json=body)
    data = resp.data

    item_id = data.get("id") if isinstance(data, dict) else data

    # retries start at 0 on enqueue; QueueItem derives attempt as retries + 1
    return QueueItem(
        item_id=item_id,
        queue_name=queue_name,
        reference=reference,
        priority=priority,
        retries=0,
        content=content,
    )


def get(queue_name: str) -> "QueueItem | None":
    """Fetch the next available item; returns None when the queue is empty."""
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    from bv.runtime.queue_item import QueueItem

    client = OrchestratorClient()
    resp = client.request("GET", "/api/queue-items/next", params={"queue_name": queue_name})
    data = resp.data
    if data is None or not isinstance(data, dict):
        return None

    queue_name_value = data.get("queue_name") or queue_name
    retries = data.get("retries", 0)

    return QueueItem(
        item_id=data.get("id"),
        queue_name=queue_name_value,
        reference=data.get("reference"),
        priority=data.get("priority"),
        retries=retries,
        content=data.get("payload"),
    )


def set_status(
    item_id: str,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Update the status/result/error for a queue item."""
    require_bv_run()
    from bv.runtime.client import OrchestratorClient

    client = OrchestratorClient()
    body = {
        "status": status,
        "result": result,
        "error_message": error,
    }
    client.request("PUT", f"/api/queue-items/{item_id}/status", json=body)
