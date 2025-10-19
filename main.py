
import os, json
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal, List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from parsers.emaestro import parse_emaestro_xml
from evaluator.rules_engine import evaluate_easa, evaluate_oma, summarise

APP_VERSION = "3.0.1"
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

def parse_zulu(date_str: str, time_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str}T{time_str}:00+00:00")

@app.post("/check-duty")
def check_single_duty(payload: DutyInput):
    rpt = parse_zulu(payload.duty_date_z, payload.report_time_z)
    end = parse_zulu(payload.duty_date_z, payload.offblocks_final_z) + timedelta(minutes=30)
    if end <= rpt:
        end = end + timedelta(days=1)
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

@app.post("/upload-roster")
async def upload_roster(file: UploadFile = File(...), crew_role: Literal["flight","cabin"]="flight"):
    raw = await file.read()
    duties_raw, meta = parse_emaestro_xml(raw)
    out = []
    skipped = []
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(BASE_TZ)
    for d in duties_raw:
        dtype = d.get("type") or "unknown"
        if dtype != "flying":
            skipped.append(d)
            continue
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
        res = {"duty_id": d.get("duty_id"), "planned_start_utc": d["planned_start_utc"],
               "planned_end_utc": d["planned_end_utc"], "sectors": sectors,
               "results":[easa, oma], "summary": summarise([easa,oma])}
        out.append(res)
    overall = "OK" if all(r["summary"]["overall_status"]=="OK" for r in out) else "Non-Compliant"
    return {"parsed_duties": len(duties_raw),
            "counts": {"flying_checked": len(out), "non_flying_skipped": len(skipped)},
            "roster_summary":{"overall_status": overall},
            "duties": out, "meta": meta}
