from typing import Any


def json_serialize_helper(o: Any):
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
