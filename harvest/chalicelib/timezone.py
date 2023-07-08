from datetime import datetime, timezone
from zoneinfo import ZoneInfo

Local = ZoneInfo("Asia/Tokyo")
UTC = timezone.utc


def now() -> datetime:
    return datetime.now(tz=Local)
