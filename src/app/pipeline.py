"""分阶段加工管线：compatibility → scope → metrics → report → compare。

每个阶段完成即写 checkpoint；进程中断后用同一输入版与冻结映射版恢复，
已完成阶段不重算。通过 CRASH_FILE 注入一次性中断，用于演示断点恢复。
"""
from __future__ import annotations

import os
import uuid

from . import ingest, models
from .models import METRIC, OBS_SUCCESS_ONLY, REASON_TEXT

DEFAULT_CRASH_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "data", "crash_once.flag")


class InterruptedByDesign(Exception):
    pass


def _maybe_crash(crash_file: str | None, job_id: str, stage: str):
    """阶段边界检查一次性中断标记。

    标记文件内容为目标阶段名时，仅在该阶段前触发；为空则在下一个阶段触发。
    命中即删除标记并抛中断，恢复时不会再炸。
    """
    if not crash_file or not os.path.exists(crash_file):
        return
    try:
        with open(crash_file, encoding="utf-8") as f:
            target = f.read().strip()
    except OSError:
        return
    if target and target != stage:
        return
    try:
        os.remove(crash_file)
    except OSError:
        pass
    raise InterruptedByDesign(f"阶段 {stage} 开始前计算中断（模拟故障）")


# --- 阶段 3：来源 × 指标相容性裁决 --------------------------------------------
def stage_compatibility(store, job, mapping_snapshot):
    jid, iv = job["job_id"], job["input_version"]
    units_by_source = store.units_by_source(jid)
    verdicts = []
    for source, m in sorted(mapping_snapshot.items()):
        ot, rp = m["observed_type"], m["retry_policy"]
        sig = models.caliber_signature(ot, rp)
        observed_units = units_by_source.get(source, set())
        if not observed_units:
            store.upsert_verdict(jid, source, ot, rp, sig, False, "NO_OBSERVATIONS")
            store.insert_exclusion(jid, source, None, "NO_OBSERVATIONS",
                                   {"source": source}, 0)
            verdicts.append((source, False, sig, "NO_OBSERVATIONS"))
            continue
        if ot == OBS_SUCCESS_ONLY:
            store.upsert_verdict(jid, source, ot, rp, sig, False,
                                 "SUCCESS_ONLY_NO_DENOMINATOR")
            # 该来源全部原始记录保留在排除账里，不进入成功率比较
            n = len(observed_units)
            store.insert_exclusion(jid, source, None, "SUCCESS_ONLY_NO_DENOMINATOR",
                                   {"observed_units": sorted(observed_units)}, n)
            verdicts.append((source, False, sig, "SUCCESS_ONLY_NO_DENOMINATOR"))
        else:
            store.upsert_verdict(jid, source, ot, rp, sig, True, None)
            verdicts.append((source, True, sig, None))
    return verdicts


# --- 阶段 4：可比样本范围加工 --------------------------------------------------
def _compatible(store, jid):
    return {v["source"]: v for v in store.list_verdicts(jid) if v["compatible"]}


def stage_scope(store, job):
    jid = job["job_id"]
    units_by_source = store.units_by_source(jid)
    compat = _compatible(store, jid)
    compat_sources = sorted(compat)

    # ---- 策略 A：收窄到共同可观察范围 ----------------------------------------
    if compat_sources:
        common = set.intersection(*(units_by_source[s] for s in compat_sources))
    else:
        common = set()
    for unit in sorted(common):
        excluded = []
        for source, m_set in units_by_source.items():
            if source in compat and unit not in m_set:  # 理论不会发生（交集）
                excluded.append({"source": source, "reason_code": "SILENT_UNIT",
                                 "detail": REASON_TEXT["SILENT_UNIT"]})
        store.save_comparable_unit(
            jid, unit, "narrow", None, compat_sources, excluded)
    # 被排除单元逐来源留痕（含成功-only 与不相容来源）
    for source, m_set in sorted(units_by_source.items()):
        for unit in sorted(m_set - common):
            reason = ("SOURCE_INCOMPATIBLE" if source not in compat
                      else "NOT_IN_COMMON_SCOPE")
            store.insert_exclusion(jid, source, unit, reason,
                                   {"strategy": "narrow", "observed_by":
                                    sorted(s for s, u in units_by_source.items() if unit in u)}, 1)
    # 共同范围内，某相容来源的静默单元（该单元在其他来源出现）也留痕
    for source in compat_sources:
        for unit in sorted(common - units_by_source[source]):
            store.insert_exclusion(jid, source, unit, "SILENT_UNIT",
                                   {"strategy": "narrow"}, 0)

    # ---- 策略 B：按口径签名分层，层内取共同范围 -------------------------------
    strata: dict[str, list[str]] = {}
    for source in compat_sources:
        strata.setdefault(compat[source]["caliber_sig"], []).append(source)
    for sig, sources in sorted(strata.items()):
        layer_common = set.intersection(*(units_by_source[s] for s in sources))
        for unit in sorted(layer_common):
            store.save_comparable_unit(jid, unit, "stratified", sig, sources, [])
        for source in sources:
            for unit in sorted(units_by_source[source] - layer_common):
                store.insert_exclusion(jid, source, unit, "NOT_IN_STRATUM_SCOPE",
                                       {"strategy": "stratified", "caliber_sig": sig}, 1)
    return {"common_units": sorted(common), "strata": strata}


# --- 阶段 5：指标计算 ----------------------------------------------------------
def stage_metrics(store, job):
    jid = job["job_id"]
    compat = _compatible(store, jid)
    agg = store.aggregated_events(jid)
    rows = {}
    for a in agg:
        unit = models.unit_key(a["bucket"], a["service"], a["region"])
        rows[(unit, a["source"])] = a

    for strategy in ("narrow", "stratified"):
        for cu in store.list_comparable_units(jid, strategy):
            unit, sig = cu["unit"], cu["stratum_sig"]
            for source in cu["sources"]:
                a = rows.get((unit, source))
                successes = a["successes"] if a else 0
                attempts = a["attempts"] if a else 0
                rate = round(successes / attempts, 6) if attempts > 0 else None
                store.upsert_metric_row(
                    jid, strategy, unit, sig, source, successes, attempts, successes, rate)
        # 不相容/成功-only 来源：只报成功量，rate 明确为不可计算（绝不补零）
        for v in store.list_verdicts(jid):
            if v["compatible"]:
                continue
            source = v["source"]
            for a in [x for x in agg if x["source"] == source]:
                unit = models.unit_key(a["bucket"], a["service"], a["region"])
                note = ("SUCCESS_ONLY_NO_DENOMINATOR" if v["observed_type"]
                        == OBS_SUCCESS_ONLY else v["reason_code"])
                store.upsert_metric_row(
                    jid, strategy, unit, v["caliber_sig"], source,
                    a["successes"], 0, a["successes"], None, note)
    return store.list_metric_rows(jid)


# --- 阶段 6：报告 --------------------------------------------------------------
def _row_view(r):
    return {
        "unit": models.unit_parts(r["unit"]),
        "source": r["source"],
        "successes": r["successes"],
        "attempts": r["attempts"],
        "success_volume": r["success_volume"],
        "rate": r["rate"],
        "rate_pct": (f"{r['rate'] * 100:.2f}%" if r["rate"] is not None else "不可计算"),
        "note": r["note"],
        "stratum_sig": r["stratum_sig"],
    }


def stage_report(store, job, scope_info):
    jid, iv, mv = job["job_id"], job["input_version"], job["mapping_version"]
    retained = store.list_retained(jid)
    exclusions = store.list_exclusions(jid)
    verdicts = []
    for v in store.list_verdicts(jid):
        d = dict(v)
        d["reason_text"] = REASON_TEXT.get(v["reason_code"]) if v["reason_code"] else None
        verdicts.append(d)

    def block(strategy):
        all_rows = store.list_metric_rows(jid, strategy)
        units = []
        for cu in store.list_comparable_units(jid, strategy):
            # 只保留实际参与该范围/该层的来源；success-only 等不可排行放 unranked
            rows = [_row_view(r) for r in all_rows
                    if r["unit"] == cu["unit"] and r["source"] in cu["sources"]]
            units.append({"unit": models.unit_parts(cu["unit"]),
                          "stratum_sig": cu["stratum_sig"],
                          "participants": cu["sources"],
                          "excluded": cu["excluded"],
                          "rows": rows})
        unranked = [_row_view(r) for r in all_rows if r["rate"] is None]
        return {"units": units, "unranked_volume_only": unranked}

    body = {
        "metric": METRIC,
        "metric_definition": models.METRIC_DEFINITION,
        "input_version": iv,
        "mapping_version": mv,
        "verdicts": verdicts,
        "scope": {
            "narrow_common_units": [models.unit_parts(u) for u in scope_info["common_units"]],
            "strata": {sig: sources for sig, sources in scope_info["strata"].items()},
        },
        "narrow_report": block("narrow"),
        "stratified_report": block("stratified"),
        "exclusions": [
            {**e, "reason_text": REASON_TEXT.get(e["reason_code"], e["reason_code"]),
             "unit": models.unit_parts(e["unit"]) if e["unit"] else None}
            for e in exclusions],
        "retained_records": [
            {**r, "reason_text": REASON_TEXT.get(r["reason_code"], r["reason_code"]),
             "detail": models.loads(r["detail"]),
             "raw": models.loads(r["raw"]) if r["raw"] else None}
            for r in retained],
        "limitations": [
            "只上报成功的来源分母不可观察，其成功率标记为不可计算，未以零失败补造分母",
            "narrow 报告仅覆盖全部相容来源共同观察到的单元，其余单元见 exclusions",
            "stratified 报告仅在同一口径签名层内排序，跨层成功率不并排比较",
            "被拒绝/被排除记录均保留原因码，可在 retained_records / exclusions 追溯",
        ],
    }
    store.save_report(jid, iv, mv, "narrow+stratified", body)
    return body


# --- 阶段 7：与上一输入版比较 --------------------------------------------------
def _previous_completed_job(store, job):
    iv_doc = store.get_input_version(job["input_version"])
    parent = iv_doc.get("parent_version")
    if not parent:
        return None, None
    row = store.conn.execute(
        "SELECT * FROM jobs WHERE input_version=? AND status='completed' ORDER BY created_at DESC",
        (parent,)).fetchone()
    if not row:
        return None, parent
    return dict(row), parent


def stage_compare(store, job):
    prev, parent = _previous_completed_job(store, job)
    current = store.get_report(job["job_id"])
    if prev is None:
        result = {"baseline_input_version": parent, "comparable": False,
                  "reason": "没有已完成的上一输入版，作为首个可比基线"}
    else:
        prev_report = store.get_report(prev["job_id"])["body"]
        cur_body = current["body"]
        prev_map = {(tuple(r["unit"]), r["source"]): r
                    for blk in ("narrow_report", "stratified_report")
                    for u in prev_report[blk]["units"] for r in u["rows"]}
        cur_map = {(tuple(r["unit"]), r["source"]): r
                   for blk in ("narrow_report", "stratified_report")
                   for u in cur_body[blk]["units"] for r in u["rows"]}
        changes, caliber_warnings = [], []
        for key in sorted(set(prev_map) | set(cur_map)):
            old, new = prev_map.get(key), cur_map.get(key)
            if old is None:
                changes.append({"unit": list(key[0]), "source": key[1], "kind": "SCOPE_ADDED",
                                "reason_text": REASON_TEXT["SCOPE_ADDED"],
                                "rate": new["rate"], "rate_pct": new["rate_pct"]})
                continue
            if new is None:
                changes.append({"unit": list(key[0]), "source": key[1], "kind": "SCOPE_DROPPED",
                                "reason_text": REASON_TEXT["SCOPE_DROPPED"],
                                "rate": old["rate"], "rate_pct": old["rate_pct"]})
                continue
            if old.get("stratum_sig") and new.get("stratum_sig") and \
                    old["stratum_sig"] != new["stratum_sig"]:
                caliber_warnings.append(
                    {"unit": list(key[0]), "source": key[1],
                     "reason_code": "CALIBER_CHANGED",
                     "reason_text": REASON_TEXT["CALIBER_CHANGED"],
                     "old_sig": old["stratum_sig"], "new_sig": new["stratum_sig"]})
            changes.append({
                "unit": list(key[0]), "source": key[1], "kind": "RATE_CHANGE",
                "old_rate": old["rate"], "new_rate": new["rate"],
                "delta": (round(new["rate"] - old["rate"], 6)
                          if old["rate"] is not None and new["rate"] is not None else None),
                "old_attempts": old["attempts"], "new_attempts": new["attempts"],
                "comparable": old["rate"] is not None and new["rate"] is not None
                              and old.get("stratum_sig") == new.get("stratum_sig"),
            })
        result = {
            "baseline_input_version": parent,
            "baseline_job": prev["job_id"],
            "comparable": True,
            "rate_changes": changes,
            "caliber_warnings": caliber_warnings,
            "limitation": "口径签名变化的行不直接比较；新增/消失单元按范围变化单独列出",
        }
    body = current["body"]
    body["comparison"] = result
    store.save_report(job["job_id"], job["input_version"], job["mapping_version"],
                      "narrow+stratified", body)
    return result


# --- 管线执行与恢复 ------------------------------------------------------------
def run_pipeline(store, iv: str, mapping_version: str | None = None,
                 crash_file: str | None = DEFAULT_CRASH_FILE) -> dict:
    iv_doc = store.get_input_version(iv)
    mv = mapping_version or iv_doc["mapping_version"]
    mapping_snapshot = store.get_mapping(mv)
    payload = iv_doc["payload"]

    job_id = f"job-{uuid.uuid4().hex[:10]}"
    store.create_job(job_id, iv, mv)
    job = store.get_job(job_id)
    return _drive(store, job, mapping_snapshot, payload, crash_file)


def resume_job(store, job_id: str, crash_file: str | None = DEFAULT_CRASH_FILE) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise KeyError(job_id)
    mapping_snapshot = store.get_mapping(job["mapping_version"])
    iv_doc = store.get_input_version(job["input_version"])
    return _drive(store, job, mapping_snapshot, iv_doc["payload"], crash_file)


def _drive(store, job, mapping_snapshot, payload, crash_file):
    jid = job["job_id"]
    runners = {
        "ingest": lambda: ingest.run_ingest(store, job, payload),
        "canonicalize": lambda: ingest.run_canonicalize(
            store, job, mapping_snapshot, payload),
        "compatibility": lambda: stage_compatibility(store, job, mapping_snapshot),
        "scope": lambda: stage_scope(store, job),
        "metrics": lambda: stage_metrics(store, job),
        "report": None,  # 需要 scope_info，见下
        "compare": lambda: stage_compare(store, job),
    }
    def checkpoint_detail(jid_, stage_):
        r = store.conn.execute(
            "SELECT detail FROM checkpoints WHERE job_id=? AND stage=?",
            (jid_, stage_)).fetchone()
        return models.loads(r["detail"]) if r and r["detail"] else None

    scope_info = None
    try:
        for stage in ingest.STAGES:
            if store.checkpoint_done(jid, stage):
                if stage == "scope":
                    scope_info = checkpoint_detail(jid, stage)
                continue
            _maybe_crash(crash_file, jid, stage)
            if stage == "report":
                if scope_info is None:
                    scope_info = checkpoint_detail(jid, "scope")
                out = stage_report(store, job, scope_info)
            else:
                out = runners[stage]()
            if stage == "scope":
                scope_info = out
            store.mark_checkpoint(jid, stage, "done",
                                  out if isinstance(out, (dict, list)) else None)
            store.update_job(jid, last_stage=stage, status="running")
    except InterruptedByDesign as exc:
        store.mark_checkpoint(jid, stage, "interrupted", {"error": str(exc)})
        store.update_job(jid, status="interrupted", last_stage=stage, error=str(exc))
        return store.get_job(jid)
    except Exception as exc:  # 真实故障同样保留现场，待 resume
        store.update_job(jid, status="failed", error=repr(exc))
        raise
    store.update_job(jid, status="completed", strategy="narrow+stratified", error=None)
    return store.get_job(jid)
