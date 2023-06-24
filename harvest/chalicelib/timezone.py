from datetime import datetime, timezone
from zoneinfo import ZoneInfo

Local = ZoneInfo("Asia/Tokyo")
UTC = timezone.utc


def now():
    return datetime.now(tz=Local)
