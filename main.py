
import json
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime, timedelta, timezone

from barc_evaluator.core import to_local, duration_hours, overlaps_window, forecast_end_from_block
from parsers.parser_emaestro import parse_emaestro_xml
from pathlib import Path

APP_DIR = Path(__file__).parent
RULES_PATH = APP_DIR / "barc_evaluator" / "rules_bundle_full.json"

app = FastAPI(title="BARC API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class NextPlannedDuty(BaseModel):
    duty_id: Optional[str] = None
    start_utc: datetime

class Duty(BaseModel):
    duty_id: Optional[str] = None
    crew_id: Optional[str] = None
    status: Literal["planned","operated"] = "planned"
    base_timezone: str = "Europe/London"
    planned_start_utc: datetime
    planned_end_utc: Optional[datetime] = None
    planned_block_minutes: Optional[int] = None
    actual_start_utc: Optional[datetime] = None
    actual_end_utc: Optional[datetime] = None
    disruption_reason: Optional[Literal["delay","diversion","disruption","other"]] = None
    last_off_duty_time_utc: Optional[datetime] = None
    next_planned_duties: List[NextPlannedDuty] = Field(default_factory=list)
    finish_location: Optional[Literal["UK","Non-UK"]] = "UK"
    sectors: Optional[int] = 1
    origin: Optional[str] = None
    destination: Optional[str] = None

class CheckRequest(BaseModel):
    rule_bundle_id: Optional[str] = "latest"
    duties: List[Duty]

def load_rules():
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def classify_night(start_utc: datetime, end_utc: datetime, tz: str):
    from barc_evaluator.core import to_local, overlaps_window, duration_hours
    s_local = to_local(start_utc, tz); e_local = to_local(end_utc, tz)
    return overlaps_window(s_local, e_local), s_local, e_local

def build_violation(duty: Duty, rule_id: str, title: str, measured: dict, entitlements: dict|None=None, citation: str|None=None, explanation: str|None=None):
    return {
        "duty_id": duty.duty_id,
        "crew_id": duty.crew_id,
        "rule_id": rule_id,
        "title": title,
        "measured": measured,
        "entitlements": entitlements or {},
        "citation": citation,
        "explanation": explanation,
        "audit": {"bundle_id":"BARC-BIDLINE-COMPLETE-v2025-10-19","checked_at": datetime.now(timezone.utc).isoformat()}
    }

@app.get("/health")
def health():
    return {"ok": True, "service":"BARC API"}

@app.get("/rules/latest")
def rules_latest():
    return JSONResponse(load_rules())

@app.post("/check-duty")
def check_duty(duty: Duty):
    tz = duty.base_timezone
    if duty.status == "planned":
        start = duty.planned_start_utc
        if duty.planned_end_utc:
            end = duty.planned_end_utc
        elif duty.planned_block_minutes:
            end = forecast_end_from_block(duty.planned_start_utc, duty.planned_block_minutes)
        else:
            raise HTTPException(400, "For planned duties provide planned_end_utc or planned_block_minutes")
    else:
        start = duty.actual_start_utc or duty.planned_start_utc
        end = duty.actual_end_utc or duty.planned_end_utc or (duty.planned_start_utc + timedelta(minutes=duty.planned_block_minutes or 0))

    imp, s_local, e_local = classify_night(start, end, tz)
    measured = {
        "status": duty.status,
        "start_local": s_local.isoformat(),
        "end_local": e_local.isoformat(),
        "duration_hours": round((end-start).total_seconds()/3600.0, 2),
        "impinges_night_window": imp
    }
    results = {"violations": [], "info": []}
    if imp:
        results["info"].append({"note":"Classified as NIGHT per BLR 10.6.2","citation":"BLR 10.6.2","measured":measured})
    else:
        results["info"].append({"note":"Does not impinge night window","measured":measured})

    # Planned forecast checks (simplified MVP)
    if duty.status == "planned" and imp and duty.next_planned_duties:
        nxt = sorted(duty.next_planned_duties, key=lambda x: x.start_utc)[0]
        within_30h = (nxt.start_utc - end) <= timedelta(hours=30)
        nl = to_local(nxt.start_utc, tz)
        el = to_local(end, tz)
        next_day_same_date = (nl.date() == el.date())
        starts_before_8 = nl.hour < 8
        if within_30h or (next_day_same_date and starts_before_8):
            results["violations"].append(build_violation(
                duty, "rest_after_night_forecast",
                "Planned rest after night may be insufficient (forecast)",
                {**measured, "next_duty_start_local": nl.isoformat()},
                citation="BLR 10.6.6.1",
                explanation="Rest should be max(30h, or next duty at/after 08:00 local next day)."
            ))

    # Operated disruption entitlement
    if duty.status == "operated" and imp and duty.disruption_reason in ("delay","diversion","disruption"):
        planned_end = duty.planned_end_utc or (duty.planned_start_utc + timedelta(minutes=duty.planned_block_minutes or 0))
        planned_imp, _, _ = classify_night(duty.planned_start_utc, planned_end, tz)
        if not planned_imp:
            drop_list = []
            for nd in duty.next_planned_duties:
                if 0 <= (nd.start_utc - end).total_seconds() <= 30*3600:
                    drop_list.append({"duty_id": nd.duty_id, "start_local": to_local(nd.start_utc, tz).isoformat()})
            ent = {"min_rest_hours":30, "drop_any_within_30h":True, "drop_list":drop_list, "day_following_return_becomes_DFD":True, "protection_credit":"PR"}
            results["violations"].append(build_violation(
                duty, "following_disruption_drop_day_entitlement",
                "Disruption → 30h rest entitlement (drop duties within 30h)",
                measured, ent, citation="BLR 10.6.6.3",
                explanation="Duty became night due to disruption; 30h recovery applies."
            ))
    return results

@app.post("/check-roster")
def check_roster(payload: CheckRequest):
    return {"results":[check_duty(d) for d in payload.duties]}

# -------- Upload roster (eMaestro XML) --------
@app.post("/upload-roster")
async def upload_roster(file: UploadFile = File(None), raw_text: str = Form(None)):
    if not file and not raw_text:
        raise HTTPException(400, "Upload an XML file or provide raw_text")
    xml_bytes = await file.read() if file else raw_text.encode("utf-8")
    try:
        duties, meta = parse_emaestro_xml(xml_bytes)
    except Exception as e:
        raise HTTPException(400, f"Failed to parse eMaestro XML: {e}")
    # Immediately evaluate the roster
    results = {"results":[check_duty(d) for d in duties], "meta": meta, "parsed_duties": len(duties)}
    return results

# Static PWA
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

@app.get("/")
def serve_index():
    return FileResponse(str(APP_DIR / "static" / "index.html"))

@app.get("/manifest.json")
def manifest():
    return FileResponse(str(APP_DIR / "static" / "manifest.json"))

@app.get("/sw.js")
def sw():
    return FileResponse(str(APP_DIR / "static" / "sw.js"))
