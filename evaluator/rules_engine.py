
from datetime import datetime, timedelta, time
from typing import Dict, Any, Optional

def _parse_hhmm(s: str):
    h, m = s.split(":"); return int(h), int(m)

def _within(t: time, start: time, end: time):
    return start <= t < end if start <= end else (t >= start or t < end)

def easa_base_limit_minutes(report_local: datetime, sectors: int, rules: Dict[str, Any]) -> int:
    t = report_local.time()
    for row in rules.get("fdp_rows", []):
        sh, sm = _parse_hhmm(row["start_local"]); eh, em = _parse_hhmm(row["end_local"])
        if _within(t, time(sh,sm), time(eh,em)):
            limits = row.get("limits", {})
            keys = sorted(map(int, limits.keys()))
            k = sectors if str(sectors) in limits else keys[-1]
            return int(limits[str(k)])
    return int(rules.get("default_limit_minutes", 660))

def easa_sector_correction(sectors: int, rules: Dict[str, Any]) -> int:
    return int(rules.get("sector_corrections", {}).get(str(sectors), 0))

def easa_wocl_penalty(report_local: datetime, rules: Dict[str, Any]) -> int:
    t = report_local.time()
    for w in rules.get("wocl", []):
        sh, sm = _parse_hhmm(w["start_local"]); eh, em = _parse_hhmm(w["end_local"])
        if _within(t, time(sh,sm), time(eh,em)):
            return int(w.get("penalty_min", 0))
    return 0

def evaluate_easa(report_local: datetime, actual_minutes: int, sectors: int, rules: Dict[str, Any]):
    base_min = easa_base_limit_minutes(report_local, sectors, rules)
    sector_adj = easa_sector_correction(sectors, rules)
    wocl_adj = easa_wocl_penalty(report_local, rules)
    legal_min = max(0, base_min + sector_adj - wocl_adj)
    flex = legal_min - actual_minutes
    res = {
        "rule_source":"EASA",
        "limits":{
            "base_limit_minutes": base_min,
            "sector_correction_minutes": sector_adj,
            "wocl_penalty_minutes": wocl_adj,
            "legal_limit_minutes": legal_min,
            "actual_minutes": actual_minutes,
            "flex_minutes": flex
        },
        "violations": [], "info":[]
    }
    if flex < 0:
        res["violations"].append({"rule_source":"EASA","rule":"fdp_exceeded","title":"FDP exceeded","excess_minutes": -flex})
    else:
        res["info"].append({"rule_source":"EASA","note":"Within FDP","flex_minutes": flex})
    return res

def oma_augmented_cap_minutes(aug_rules: Dict[str, Any], sectors: int, rest_class: int, augmented_pilots: int, long_sector_over_9h: bool) -> int:
    if augmented_pilots not in (1,2): return 0
    rc = str(rest_class)
    if rc not in aug_rules: return 0
    key = "upto3" if sectors <= 3 else "two_or_less_one_over_9h" if (sectors <= 2 and long_sector_over_9h) else "upto3"
    cap_hours = aug_rules[rc][str(augmented_pilots)].get(key)
    return int(float(cap_hours)*60) if cap_hours else 0

def evaluate_oma(crew_role: str, report_utc: datetime, end_utc: datetime, sectors: int, rest_class: Optional[int], augmented_pilots: Optional[int], long_sector_over_9h: bool, inflight_rest_minutes: int, rules: Dict[str, Any], base_tz: str):
    out = {"rule_source":"OMA","info":[],"violations":[],"limits":{}}
    if not (report_utc and end_utc): 
        out["violations"].append({"rule_source":"OMA","rule":"inputs_missing","title":"Report and end times required"})
        return out
    actual_min = int((end_utc - report_utc).total_seconds()/60.0)
    if crew_role == "cabin":
        rest_class = rest_class or 0
        table = rules.get("cabin_crew", {}).get("min_rest_by_extended_fdp", [])
        req = None; not_allowed=False
        fdp_hours = actual_min/60.0
        for row in table:
            if rest_class in row["classes"] and row["min_fdp_h"] <= fdp_hours <= row["max_fdp_h"]:
                req = row["min_rest_minutes"]
                if req is None: not_allowed=True
                break
        out["limits"] = {"extended_fdp_hours": round(fdp_hours,2), "required_inflight_rest_minutes": req, "provided_inflight_rest_minutes": inflight_rest_minutes}
        if not_allowed:
            out["violations"].append({"rule_source":"OMA","rule":"rest_class_not_allowed","title":"Rest class not permitted for this FDP length"})
        elif req is not None and inflight_rest_minutes < int(req):
            out["violations"].append({"rule_source":"OMA","rule":"insufficient_inflight_rest","title":"Minimum in‑flight rest not met","shortfall_minutes": int(req)-inflight_rest_minutes})
        else:
            out["info"].append({"rule_source":"OMA","note":"Cabin in‑flight rest requirement satisfied"})
        return out
    if (augmented_pilots or 0)>0 and (rest_class or 0) in (1,2):
        cap = oma_augmented_cap_minutes(rules.get("augmentation_caps",{}), sectors, int(rest_class), int(augmented_pilots or 0), bool(long_sector_over_9h))
        flex = cap - actual_min
        out["limits"] = {"augmented_cap_minutes": cap, "actual_minutes": actual_min, "flex_minutes": flex}
        if flex < 0:
            out["violations"].append({"rule_source":"OMA","rule":"augmented_cap_exceeded","title":"Augmented FDP cap exceeded","excess_minutes": -flex})
        else:
            out["info"].append({"rule_source":"OMA","note":"Within augmented FDP cap","flex_minutes": flex})
        return out
    out["info"].append({"rule_source":"OMA","note":"No augmented/cabin constraints applied"})
    out["limits"] = {"actual_minutes": actual_min}
    return out

def evaluate_home_standby(start_utc: datetime, end_utc: datetime, contacted_utc: Optional[datetime], report_utc: Optional[datetime]) -> Dict[str, Any]:
    out = {"rule_source":"OMA","mode":"home_standby","violations":[],"info":[],"limits":{}}
    total = int((end_utc - start_utc).total_seconds()//60)
    if total > 16*60:
        out["violations"].append({"rule":"home_standby_exceeds_16h","title":"Home Standby exceeds 16 hours","excess_minutes": total-16*60})
    reduction = 0
    if contacted_utc and report_utc:
        cur = start_utc
        counted = 0
        while cur < contacted_utc:
            lt = (cur.hour*60+cur.minute)
            if 23*60 <= lt or lt < 7*60:
                pass
            else:
                counted += 1
            cur = cur + timedelta(minutes=1)
        if counted > 6*60:
            reduction = counted - 6*60
        out["limits"]["fdp_reduction_minutes"] = reduction
        out["info"].append({"note":"FDP reduction due to late call from Home Standby","reduction_minutes": reduction})
    else:
        out["info"].append({"note":"No call to duty during Home Standby"})
    out["limits"]["standby_minutes"] = total
    return out

def evaluate_airport_standby(start_utc: datetime, end_utc: datetime, assigned_report_utc: Optional[datetime]) -> Dict[str, Any]:
    out = {"rule_source":"OMA","mode":"airport_standby","violations":[],"info":[],"limits":{}}
    standby_min = int((end_utc - start_utc).total_seconds()//60)
    out["limits"]["standby_minutes"] = standby_min
    if assigned_report_utc:
        combined = standby_min + int(((end_utc if end_utc>assigned_report_utc else assigned_report_utc) - assigned_report_utc).total_seconds()//60)
        out["limits"]["combined_ceiling_minutes"] = 16*60
        if combined > 16*60:
            out["violations"].append({"rule":"airport_standby_plus_fdp_gt_16h","title":"Airport Standby + FDP exceeds 16h","excess_minutes": combined-16*60})
        else:
            out["info"].append({"note":"Airport Standby + FDP within 16h ceiling","combined_minutes": combined})
    else:
        out["info"].append({"note":"No assignment from Airport Standby"})
    return out

def evaluate_reserve_day(start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    out = {"rule_source":"OMA","mode":"reserve","violations":[],"info":[{"note":"Reserve day recorded"}],"limits":{
        "reserve_span_minutes": int((end_utc - start_utc).total_seconds()//60)
    }}
    return out

def summarise(results: list[dict]) -> dict:
    overall = "OK"
    if any(r.get("violations") for r in results):
        overall = "Non-Compliant"
    return {"overall_status": overall}
