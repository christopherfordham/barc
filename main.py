
import os, json
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal, List, Dict, Any
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from parsers.emaestro import parse_emaestro_xml
from evaluator.rules_engine import (
    evaluate_easa, evaluate_oma, summarise,
    evaluate_home_standby, evaluate_airport_standby, evaluate_reserve_day
)

APP_VERSION = "3.1.0"
BASE_TZ = os.environ.get("BARC_BASE_TZ","Europe/London")

def load_rules(name: str):
    here = os.path.dirname(__file__)
    path = os.path.join(here, "rules", name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
EASA = load_rules("easa_fdp.json")
OMA = load_rules("oma_rules.json")

app = FastAPI(title="BARC API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True, "service": "BARC API", "version": APP_VERSION}

# ===== Single Duty =====
class DutyInput(BaseModel):
    duty_date_z: str = Field(..., description="YYYY-MM-DD (Zulu)")
    report_time_z: str = Field(..., description="HH:MM (Zulu)")
    offblocks_final_z: str = Field(..., description="HH:MM (Zulu)")
    num_sectors: int = Field(..., ge=1, le=10)
    crew_role: Literal["flight","cabin"] = "flight"
    augmented_pilots: Optional[int] = 0
    rest_facility_class: Optional[int] = None
    long_sector_over_9h: Optional[bool] = False
    inflight_rest_minutes: Optional[int] = 0

    @field_validator("report_time_z","offblocks_final_z")
    @classmethod
    def _hhmm(cls, v: str):
        try:
            h,m = map(int, v.split(":"))
            assert 0 <= h <= 23 and 0 <= m <= 59
        except Exception:
            raise ValueError("Time must be HH:MM in Zulu")
        return v

def parse_zulu(date_str: str, time_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str}T{time_str}:00+00:00")

@app.post("/check-duty")
def check_single_duty(payload: DutyInput):
    rpt = parse_zulu(payload.duty_date_z, payload.report_time_z)
    end = parse_zulu(payload.duty_date_z, payload.offblocks_final_z) + timedelta(minutes=30)
    if end <= rpt:
        end = end + timedelta(days=1)
    if (end - rpt) > timedelta(hours=24, minutes=30):
        raise HTTPException(status_code=400, detail="Single-duty span exceeds 24h; check inputs.")
    actual_min = int((end - rpt).total_seconds()//60)
    try:
        from zoneinfo import ZoneInfo
        rpt_local = rpt.astimezone(ZoneInfo(BASE_TZ))
    except Exception:
        rpt_local = rpt
    easa = evaluate_easa(rpt_local, actual_min, payload.num_sectors, EASA)
    oma = evaluate_oma(payload.crew_role, rpt, end, payload.num_sectors,
                       payload.rest_facility_class, payload.augmented_pilots,
                       bool(payload.long_sector_over_9h), int(payload.inflight_rest_minutes or 0),
                       OMA, BASE_TZ)
    results = [easa, oma]
    return {"inputs": payload.model_dump(), "results": results, "summary": summarise(results)}

# ===== Standby / Reserve endpoints =====
class HomeStandbyInput(BaseModel):
    start_date_z: str
    start_time_z: str
    end_date_z: str
    end_time_z: str
    contacted_date_z: Optional[str] = None
    contacted_time_z: Optional[str] = None
    report_date_z: Optional[str] = None
    report_time_z: Optional[str] = None

def _z(d,t): return datetime.fromisoformat(f"{d}T{t}:00+00:00")

@app.post("/check-standby/home")
def check_home_standby(payload: HomeStandbyInput):
    start = _z(payload.start_date_z, payload.start_time_z)
    end   = _z(payload.end_date_z,   payload.end_time_z)
    contacted = _z(payload.contacted_date_z, payload.contacted_time_z) if payload.contacted_date_z and payload.contacted_time_z else None
    report    = _z(payload.report_date_z, payload.report_time_z) if payload.report_date_z and payload.report_time_z else None
    if end <= start:
        end = end + timedelta(days=1)
    return evaluate_home_standby(start, end, contacted, report)

class AirportStandbyInput(BaseModel):
    start_date_z: str
    start_time_z: str
    end_date_z: str
    end_time_z: str
    assigned_report_date_z: Optional[str] = None
    assigned_report_time_z: Optional[str] = None

@app.post("/check-standby/airport")
def check_airport_standby(payload: AirportStandbyInput):
    start = _z(payload.start_date_z, payload.start_time_z)
    end   = _z(payload.end_date_z,   payload.end_time_z)
    assigned = _z(payload.assigned_report_date_z, payload.assigned_report_time_z) if payload.assigned_report_date_z and payload.assigned_report_time_z else None
    if end <= start:
        end = end + timedelta(days=1)
    return evaluate_airport_standby(start, end, assigned)

class ReserveInput(BaseModel):
    start_date_z: str
    start_time_z: str
    end_date_z: str
    end_time_z: str

@app.post("/check-reserve")
def check_reserve(payload: ReserveInput):
    start = _z(payload.start_date_z, payload.start_time_z)
    end   = _z(payload.end_date_z,   payload.end_time_z)
    if end <= start:
        end = end + timedelta(days=1)
    return evaluate_reserve_day(start, end)

# ===== Roster upload (now includes ground-duty legality + FTL interfacing) =====
def _pair_ground_with_next_flying(ground: Dict[str, Any], flying_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # Return the next flying duty if it begins within 0..8h after ground end.
    ge = datetime.fromisoformat(ground["planned_end_utc"].replace("Z","+00:00"))
    best = None; best_dt = None
    for f in flying_list:
        fs = datetime.fromisoformat(f["planned_start_utc"].replace("Z","+00:00"))
        if fs >= ge:
            delta = fs - ge
            if timedelta(0) <= delta <= timedelta(hours=8):
                if best is None or delta < best_dt:
                    best = f; best_dt = delta
    return best

@app.post("/upload-roster")
async def upload_roster(file: UploadFile = File(...), crew_role: Literal["flight","cabin"]="flight"):
    raw = await file.read()
    duties_raw, meta = parse_emaestro_xml(raw)

    # Split flying vs ground-like
    flying_src: List[Dict[str, Any]] = []
    ground_src: List[Dict[str, Any]] = []
    for d in duties_raw:
        if (d.get("type") or "unknown") == "flying":
            flying_src.append(d)
        else:
            ground_src.append(d)

    # Evaluate flying
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(BASE_TZ)
    flying_out = []
    for d in flying_src:
        start = datetime.fromisoformat(d["planned_start_utc"].replace("Z","+00:00"))
        end = datetime.fromisoformat(d["planned_end_utc"].replace("Z","+00:00"))
        sectors = int(d.get("sectors") or 1)
        rpt_local = start.astimezone(tz)
        easa = evaluate_easa(rpt_local, int((end-start).total_seconds()//60), sectors, EASA)
        oma  = evaluate_oma(crew_role, start, end, sectors,
                            int(d.get("rest_facility_class") or 0) if d.get("rest_facility_class") else None,
                            int(d.get("augmented_pilots") or 0) if d.get("augmented_pilots") else 0,
                            bool(d.get("long_sector_over_9h") or False),
                            int(d.get("inflight_rest_minutes") or 0),
                            OMA, BASE_TZ)
        flying_out.append({
            "type":"flying",
            "duty_id": d.get("duty_id"),
            "planned_start_utc": d["planned_start_utc"],
            "planned_end_utc": d["planned_end_utc"],
            "sectors": sectors,
            "results":[easa, oma],
            "summary": summarise([easa,oma])
        })

    # Evaluate ground with FTL interfacing
    ground_out = []
    for g in ground_src:
        gtype = g.get("type")
        start = datetime.fromisoformat(g["planned_start_utc"].replace("Z","+00:00"))
        end   = datetime.fromisoformat(g["planned_end_utc"].replace("Z","+00:00"))
        paired = _pair_ground_with_next_flying(g, flying_src)
        if gtype == "standby_home":
            contacted = None; report = None; applied_to = None
            if paired:
                report = datetime.fromisoformat(paired["planned_start_utc"].replace("Z","+00:00"))
                contacted = report - timedelta(hours=2)
                applied_to = paired.get("duty_id")
            eval_res = evaluate_home_standby(start, end, contacted, report)
            if applied_to: eval_res["applied_to_duty_id"] = applied_to
            ground_out.append({"type":"standby_home", "planned_start_utc": g["planned_start_utc"], "planned_end_utc": g["planned_end_utc"], "label": g.get("label"), "results":[eval_res], "summary": summarise([eval_res])})
        elif gtype == "standby_airport":
            assigned = None; applied_to = None
            if paired:
                assigned = datetime.fromisoformat(paired["planned_start_utc"].replace("Z","+00:00"))
                applied_to = paired.get("duty_id")
            eval_res = evaluate_airport_standby(start, end, assigned)
            if applied_to: eval_res["applied_to_duty_id"] = applied_to
            ground_out.append({"type":"standby_airport", "planned_start_utc": g["planned_start_utc"], "planned_end_utc": g["planned_end_utc"], "label": g.get("label"), "results":[eval_res], "summary": summarise([eval_res])})
        elif gtype == "reserve":
            eval_res = evaluate_reserve_day(start, end)
            ground_out.append({"type":"reserve", "planned_start_utc": g["planned_start_utc"], "planned_end_utc": g["planned_end_utc"], "label": g.get("label"), "results":[eval_res], "summary": summarise([eval_res])})
        else:
            ground_out.append({"type": gtype or "ground", "planned_start_utc": g["planned_start_utc"], "planned_end_utc": g["planned_end_utc"], "label": g.get("label"), "results":[], "summary":{"overall_status":"OK"}})

    # Merge output list in chronological order
    def _key(dtstr): return datetime.fromisoformat(dtstr.replace("Z","+00:00"))
    all_out = flying_out + ground_out
    all_out.sort(key=lambda r: _key(r["planned_start_utc"]))

    # Overall summary considers both flying and ground legality
    overall = "OK"
    for item in all_out:
        if item.get("summary",{}).get("overall_status") == "Non-Compliant":
            overall = "Non-Compliant"; break

    return {
        "parsed_duties": len(duties_raw),
        "counts": {
            "flying_checked": len(flying_out),
            "non_flying_evaluated": len(ground_out)
        },
        "roster_summary": {"overall_status": overall},
        "duties": all_out,
        "meta": meta,
        "assumptions": {
            "home_standby_contact_offset_minutes": 120,
            "pairing_window_hours_after_ground_end": 8
        }
    }
