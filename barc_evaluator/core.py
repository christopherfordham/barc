
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

NIGHT_START = time(0,59)
NIGHT_END = time(4,59)

def to_local(dt_utc, tz_str):
    return dt_utc.astimezone(ZoneInfo(tz_str))

def duration_hours(a,b):
    return (b-a).total_seconds()/3600.0

def overlaps_window(start_local, end_local, ws=NIGHT_START, we=NIGHT_END):
    cur = start_local
    while cur < end_local:
        d = cur.date()
        wstart = datetime.combine(d, ws, tzinfo=start_local.tzinfo)
        wend = datetime.combine(d, we, tzinfo=start_local.tzinfo)
        if max(start_local, wstart) < min(end_local, wend):
            return True
        cur = datetime.combine(d, time(0,0), tzinfo=start_local.tzinfo) + timedelta(days=1)
    return False

def forecast_end_from_block(actual_start_utc, block_minutes):
    return actual_start_utc + timedelta(minutes=block_minutes)
