from datetime import datetime

import pytz

Local = pytz.timezone('Asia/Tokyo')


def now():
    return datetime.now(tz=pytz.UTC).astimezone(Local)
