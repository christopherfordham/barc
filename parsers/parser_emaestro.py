
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

def _parse_dt(val: str) -> Optional[datetime]:
    if not val:
        return None
    # Try a few common formats (UTC Z; basic ISO)
    for fmt in ("%Y-%m-%dT%H:%M:%SZ","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%MZ","%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(val, fmt)
            # assume UTC if 'Z' or no tz given
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _text(elem, name):
    x = elem.find(name)
    return x.text.strip() if (x is not None and x.text) else None

def _attr(elem, name):
    return elem.attrib.get(name)

def parse_emaestro_xml(xml_bytes: bytes) -> Tuple[List[Dict], Dict]:
    """Parse an eMaestro RosterFile/TripFile export.
    Returns (duties, meta) where duties is a list of normalized dicts suitable for /check-roster.
    We try to be permissive: extract times from attributes or child nodes commonly seen in BA exports.
    """
    tree = ET.fromstring(xml_bytes)

    ns = ""  # not using namespaces for permissive parse
    duties = []
    crew_id = None
    period_start = None
    period_end = None

    # Extract header info if present
    header = tree.find(".//Header") or tree.find(".//ROSTERHEADER") or tree.find(".//RosterHeader")
    if header is not None:
        crew_id = _text(header, "CrewId") or _text(header, "CREWID") or _text(header, "EmployeeId")
        period_start = _text(header, "PeriodStart") or _text(header, "STARTDATE")
        period_end = _text(header, "PeriodEnd") or _text(header, "ENDDATE")

    # Iterate roster duties (common containers)
    # Try multiple paths to be resilient
    duty_nodes = tree.findall(".//RosterDuty") + tree.findall(".//DUTY") + tree.findall(".//Duty")
    for dn in duty_nodes:
        # Attributes sometimes include UTC start/end
        s = _attr(dn, "StartUTC") or _text(dn, "StartUTC") or _text(dn, "SignOnUTC") or _text(dn, "StartTimeUTC")
        e = _attr(dn, "EndUTC") or _text(dn, "EndUTC") or _text(dn, "SignOffUTC") or _text(dn, "EndTimeUTC")
        # If only local times present we cannot safely infer TZ here; skip if we cannot parse UTC
        start = _parse_dt(s) if s else None
        end = _parse_dt(e) if e else None
        # For robustness, also check nested flight legs
        sectors = 0
        origin = _text(dn, "Departure") or _text(dn, "From") or None
        destination = _text(dn, "Arrival") or _text(dn, "To") or None

        flight_nodes = dn.findall(".//Flight") + dn.findall(".//SECTOR") + dn.findall(".//Leg")
        for fn in flight_nodes:
            sectors += 1
            origin = _text(fn, "From") or _text(fn, "Departure") or origin
            destination = _text(fn, "To") or _text(fn, "Arrival") or destination
            fs = _attr(fn, "STDUTC") or _text(fn, "STDUTC") or _text(fn, "STD") or None
            fe = _attr(fn, "STAUTC") or _text(fn, "STAUTC") or _text(fn, "STA") or None
            # Use earliest/latest from legs if duty start/end absent
            if start is None and fs:
                start = _parse_dt(fs) or start
            if end is None and fe:
                end = _parse_dt(fe) or end

        # If still missing times, skip the duty (cannot evaluate)
        if start and end and end > start:
            duties.append({
                "duty_id": _attr(dn, "Id") or _text(dn, "DutyId") or _text(dn, "ID") or None,
                "crew_id": crew_id,
                "status": "planned",
                "base_timezone": "Europe/London",
                "planned_start_utc": start.isoformat().replace("+00:00","Z"),
                "planned_end_utc": end.isoformat().replace("+00:00","Z"),
                "planned_block_minutes": None,
                "actual_start_utc": None,
                "actual_end_utc": None,
                "disruption_reason": None,
                "next_planned_duties": [],
                "finish_location": "UK",
                "sectors": sectors or 1,
                "origin": origin,
                "destination": destination
            })

    meta = {
        "crew_id": crew_id,
        "period_start": period_start,
        "period_end": period_end
    }
    return duties, meta
