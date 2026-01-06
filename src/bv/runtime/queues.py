from __future__ import annotations

from bv.runtime._guard import require_bv_run


def list():
    """List queue names."""
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    client = OrchestratorClient()
    resp = client.request("GET", "/api/queues")
    data = resp.data
    
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        items = data.get("items")
    elif isinstance(data, list):
        items = data
    else:
        items = []
    
    queue_names = []
    for item in items:
        if isinstance(item, str):
            queue_names.append(item)
            continue
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("queue_name")
        if name:
            queue_names.append(str(name))
    
    return queue_names


def put(queue_name: str, payload: dict, reference: str | None = None):
    """Legacy alias for add_queue_item."""
    require_bv_run()
    return add_queue_item(queue_name, payload, reference)


def get(queue_name: str):
    """Legacy alias for get_queue_item."""
    require_bv_run()
    return get_queue_item(queue_name)


def add_queue_item(queue_name: str, payload: dict, reference: str | None = None) -> str:
    """Add a new item to a queue by name."""
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    client = OrchestratorClient()
    body = {
        "queue_name": queue_name,
        "payload": payload,
        "reference": reference
    }
    resp = client.request("POST", "/api/queue-items/add", json=body)
    return str(resp.data.get("id") or "")


def get_queue_item(queue_name: str) -> "QueueItem | None":
    """Fetch the next available item from a queue and mark it as IN_PROGRESS.

    Returns None when the queue is empty; otherwise a typed, immutable QueueItem.
    """
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    from bv.runtime.queue_item import QueueItem

    client = OrchestratorClient()
    resp = client.request("GET", "/api/queue-items/next", params={"queue_name": queue_name})
    data = resp.data
    if data is None:
        return None
    if not isinstance(data, dict):
        return None

    # Map backend fields to the typed object; payload stays parsed as provided.
    item_id = data.get("id")
    queue_name_value = data.get("queue_name") or queue_name
    reference = data.get("reference") if data.get("reference") is not None else None
    priority = data.get("priority")
    retries = data.get("retries", 0)
    content = data.get("payload")

    return QueueItem(
        item_id=item_id,
        queue_name=queue_name_value,
        reference=reference,
        priority=priority,
        retries=retries,
        content=content,
    )


def set_queue_item_status(
    item_id: str, 
    status: str, 
    result: dict | None = None, 
    error_message: str | None = None
) -> None:
    """Update the status, result, or error message of a queue item."""
    require_bv_run()
    from bv.runtime.client import OrchestratorClient
    client = OrchestratorClient()
    body = {
        "status": status,
        "result": result,
        "error_message": error_message
    }
    client.request("PUT", f"/api/queue-items/{item_id}/status", json=body)

