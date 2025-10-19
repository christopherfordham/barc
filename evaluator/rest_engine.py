
from datetime import datetime, timedelta, time
from typing import Dict, Any, Optional

# Default base airports for BA
DEFAULT_BASE_AIRPORTS = {"LHR","LGW","LCY"}

def _fmt_hm(minutes: int) -> str:
    sign = "-" if minutes < 0 else ""
    m = abs(int(minutes))
    h = m // 60
    mm = m % 60
    return f"{sign}{h}h {mm:02d}m"

def _overlaps_wocl(start_local: datetime, end_local: datetime) -> bool:
    # WOCL 02:00–05:59 local
    cur = start_local
    while cur <= end_local:
        if time(2,0) <= cur.time() < time(6,0):
            return True
        cur += timedelta(minutes=15)
    return False

def _policy_required_rest(prev_duty_minutes: int, policy: str) -> int:
    policy = (policy or "OMA_HOME").upper()
    if policy == "EASA":
        base = 600
    elif policy == "OMA_AWAY":
        base = 600
    else:
        base = 720  # OMA_HOME
    return max(base, int(prev_duty_minutes))

def _select_policy(prev_end_airport: Optional[str], next_start_airport: Optional[str], base_airports: set) -> str:
    a = (prev_end_airport or "").upper()
    b = (next_start_airport or "").upper()
    if not a and not b:
        return "OMA_HOME"
    if a in base_airports and b in base_airports:
        return "OMA_HOME"
    return "OMA_AWAY"

def evaluate_rest_enhanced(prev_start_utc: datetime,
                           prev_end_utc: datetime,
                           next_report_utc: datetime,
                           prev_end_airport: Optional[str],
                           next_start_airport: Optional[str],
                           base_airports: Optional[set] = None,
                           base_tz: Optional[str] = "Europe/London",
                           apply_wocl: bool = True,
                           apply_travel: bool = True,
                           prefer_oma: bool = True) -> Dict[str, Any]:
    """Return detailed rest evaluation with WOCL + Travel + Home/Away policy."""
    from zoneinfo import ZoneInfo

    base_airports = base_airports or DEFAULT_BASE_AIRPORTS
    policy = _select_policy(prev_end_airport, next_start_airport, base_airports)
    if not prefer_oma and policy != "EASA":
        policy = "EASA"

    prev_duty_minutes = int((prev_end_utc - prev_start_utc).total_seconds()//60)
    required = _policy_required_rest(prev_duty_minutes, policy)
    adjustments = []
    total_adj = 0

    try:
        tz = ZoneInfo(base_tz or "Europe/London")
    except Exception:
        tz = ZoneInfo("UTC")
    prev_start_local = prev_start_utc.astimezone(tz)
    prev_end_local = prev_end_utc.astimezone(tz)

    wocl_overlap = False
    if apply_wocl:
        wocl_overlap = _overlaps_wocl(prev_start_local, prev_end_local)
        if wocl_overlap:
            if policy == "EASA":
                add = 120; total_adj += add
                adjustments.append({"type":"WOCL","minutes":add,"note":"WOCL overlap: +2h (EASA)"})
            elif policy == "OMA_HOME":
                add = 60; total_adj += add
                adjustments.append({"type":"WOCL","minutes":add,"note":"WOCL overlap: +1h (OMA Home)"})
            elif policy == "OMA_AWAY":
                add = 30; total_adj += add
                adjustments.append({"type":"WOCL","minutes":add,"note":"WOCL overlap: +30m (OMA Away)"})
    travel_adjust = 0
    if apply_travel and policy == "OMA_AWAY":
        travel_adjust = 60; total_adj += travel_adjust
        adjustments.append({"type":"TRAVEL","minutes":travel_adjust,"note":"Travel-time (away): +1h"})

    required_final = required + total_adj
    actual = int((next_report_utc - prev_end_utc).total_seconds()//60)
    shortfall = max(0, required_final - actual)
    next_earliest = prev_end_utc + timedelta(minutes=required_final)

    if shortfall > 30:
        color = "red"; status = "Non-Compliant"
    elif shortfall > 0:
        color = "amber"; status = "At-Risk"
    else:
        color = "green"; status = "OK"

    notes = [{"rule_source":"REST","detail":f"Base requirement {_fmt_hm(required)} ({policy})"}]
    for adj in adjustments:
        notes.append({"rule_source":"REST","detail":adj["note"]})
    if shortfall:
        notes.append({"rule_source":"REST","detail":f"Rest short by {_fmt_hm(shortfall)}"})

    return {
        "actual_rest_minutes": actual,
        "required_rest_minutes": required_final,
        "shortfall_minutes": shortfall,
        "next_earliest_report_z": next_earliest.isoformat().replace("+00:00","Z"),
        "rest_status": status,
        "color_code": color,
        "policy": policy,
        "wocl_overlap": wocl_overlap,
        "travel_adjustment_minutes": travel_adjust,
        "adjustments": adjustments,
        "notes": notes
    }
