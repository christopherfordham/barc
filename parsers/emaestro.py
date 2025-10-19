
import re, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LHR_TZ = ZoneInfo("Europe/London")
UTC = timezone.utc

def safe_decode_xml(data: bytes) -> str:
    data = data.lstrip(b"\xef\xbb\xbf\xff\xfe\xfe\xff\r\n\t \x00")
    i = data.find(b'<')
    if i > 0:
        data = data[i:]
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16le","utf-16be","windows-1252","latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("latin-1", errors="ignore")

def split_composite_xml(text: str):
    cleaned = re.sub(r"^\s*\[(?:ROSTER|TRIP)\]\s*$", "", text, flags=re.MULTILINE)
    cleaned = cleaned.lstrip()
    parts = re.split(r"(?=(?:<\?xml\s+version=))", cleaned)
    if len(parts) == 1:
        parts = re.split(r"(?=(?:<tfs:TripFileSpecification\b))", cleaned)
    xmls = []
    for p in parts:
        s = p.strip()
        if not s:
            continue
        if s.startswith("<?xml") or s.startswith("<rfs:") or s.startswith("<tfs:"):
            xmls.append(s)
    return xmls

def _iso_local_date_time_to_utc(date_iso: str, time_hhmm: str):
    if time_hhmm.strip() == "24:00":
        base = datetime.fromisoformat(date_iso) + timedelta(days=1)
        dt = datetime.combine(base.date(), datetime.strptime("00:00", "%H:%M").time(), tzinfo=LHR_TZ)
        return dt.astimezone(UTC)
    base = datetime.fromisoformat(date_iso)
    hh, mm = map(int, time_hhmm.split(":"))
    dt = datetime(base.year, base.month, base.day, hh, mm, tzinfo=LHR_TZ)
    return dt.astimezone(UTC)

def _parse_iso8601_duration_to_minutes(s: str) -> int:
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?", s.strip())
    if not m:
        return 0
    h = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    return h*60 + mins

def _localname(tag: str) -> str:
    return tag.split('}',1)[-1] if '}' in tag else tag

def _iter_by_localname(root, name):
    for el in root.iter():
        if _localname(el.tag) == name:
            yield el

def _find_child(el, name):
    for ch in el:
        if _localname(ch.tag) == name:
            return ch
    return None

def _find_child_text(el, name):
    ch = _find_child(el, name)
    return ch.text if ch is not None else None

def parse_roster_tree(root):
    duties = []
    for rd in _iter_by_localname(root, "RosterDuty"):
        gd = _find_child(rd, "GroundDuty")
        td = _find_child(rd, "TripDuty")
        if gd is not None:
            sd = _find_child_text(gd, "StartDate"); st = _find_child_text(gd, "StartTime")
            ed = _find_child_text(gd, "EndDate");   et = _find_child_text(gd, "EndTime")
            if sd and st and ed and et:
                start = _iso_local_date_time_to_utc(sd.strip(), st.strip())
                end   = _iso_local_date_time_to_utc(ed.strip(), et.strip())
                if end > start:
                    duties.append({
                        "type": "ground",
                        "status": "planned",
                        "base_timezone": "Europe/London",
                        "planned_start_utc": start.isoformat().replace("+00:00","Z"),
                        "planned_end_utc": end.isoformat().replace("+00:00","Z"),
                        "sectors": 0,
                        "crew_role": "flight",
                        "source": "roster:ground"
                    })
        elif td is not None:
            tripno = _find_child_text(td, "TripIdentifier")
            sd = _find_child_text(td, "StartDate")
            duties.append({
                "marker": True,
                "type": "trip_marker",
                "trip_number": (tripno or "").strip(),
                "trip_start_date": (sd or "").strip(),
                "source": "roster:trip_marker"
            })
    meta = {}
    hdr = next(_iter_by_localname(root, "RosterFileHeader"), None)
    if hdr is not None:
        meta = {"file":"roster","file_name": _find_child_text(hdr, "FileName")}
    return duties, meta

def parse_trip_tree(root):
    duties = []
    for trip in _iter_by_localname(root, "Trip"):
        td = _find_child(trip, "TripDetails")
        ts = _find_child(trip, "TripSpanDetails")
        trip_number = _find_child_text(td, "TripNumber") if td is not None else None
        span_start = _find_child_text(ts, "StartDate") if ts is not None else None
        for dn in trip:
            if _localname(dn.tag) != "Duty":
                continue
            dd = _find_child(dn, "DutyDetails")
            if dd is None or not span_start:
                continue
            duty_no = (_find_child_text(dd, "DutyNumber") or "").strip()
            report = (_find_child_text(dd, "ActualReportTime") or "00:00").strip()
            dur = (_find_child_text(dd, "DutyHours") or "PT00H00M").strip()
            sectors_text = (_find_child_text(dd, "NumberOfSectors") or "1").strip()
            sectors = int(sectors_text) if sectors_text.isdigit() else 1

            # first sector's RelativeDepartureDay, default 0
            rel_day = 0
            for sec in dn:
                if _localname(sec.tag) == "Sector":
                    sdet = _find_child(sec, "SectorDetails")
                    if sdet is not None:
                        rdd = _find_child_text(sdet, "RelativeDepartureDay")
                        if rdd and rdd.strip().lstrip("+-").isdigit():
                            rel_day = int(rdd.strip())
                    break

            base_date = datetime.fromisoformat(span_start) + timedelta(days=rel_day)
            rh, rm = map(int, report.split(":"))
            report_local = base_date.replace(hour=rh, minute=rm, tzinfo=LHR_TZ)
            report_utc = report_local.astimezone(UTC)
            minutes = _parse_iso8601_duration_to_minutes(dur)
            end_utc = report_utc + timedelta(minutes=minutes)

            duties.append({
                "type": "flying",
                "duty_id": f"{trip_number}-{duty_no}",
                "status": "planned",
                "base_timezone": "Europe/London",
                "planned_start_utc": report_utc.isoformat().replace("+00:00","Z"),
                "planned_end_utc": end_utc.isoformat().replace("+00:00","Z"),
                "sectors": sectors,
                "crew_role": "flight",
                "source": "trip:duty"
            })
    return duties, {"file":"trip"}

def parse_emaestro_xml(data: bytes):
    text = safe_decode_xml(data)
    xml_chunks = split_composite_xml(text)
    if not xml_chunks:
        raise ValueError("No XML documents detected (expecting Roster/Trip XML)")
    all_duties = []; metas = []
    for chunk in xml_chunks:
        chunk = chunk.lstrip()
        if not chunk.startswith("<"):
            lt = chunk.find("<")
            if lt != -1:
                chunk = chunk[lt:]
        root = ET.fromstring(chunk)
        tag = _localname(root.tag)
        if "RosterFileSpecification" in tag:
            d, m = parse_roster_tree(root)
            all_duties.extend(d); metas.append(m)
        elif "TripFileSpecification" in tag:
            d, m = parse_trip_tree(root)
            all_duties.extend(d); metas.append(m)
        else:
            continue
    duties = [d for d in all_duties if not d.get("marker")]
    return duties, {"parts": metas}
