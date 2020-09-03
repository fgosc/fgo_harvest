from datetime import datetime, timedelta, timezone

Local = timezone(timedelta(hours=+9), 'JST')
UTC = timezone.utc


def now():
    return datetime.now(tz=UTC).astimezone(Local)
