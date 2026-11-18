"""输入版本快照与原始记录规范化。

输入版是只追加的完整快照（父版本号记录血缘）；每条原始记录要么成为规范事件，
要么带原因码进入 retained_records。绝不补零、绝不静默丢弃。
"""
from __future__ import annotations

from . import models
from .models import (
    CANONICAL_CORE, CANONICAL_OUTCOME, OBS_FULL, OBS_SUCCESS_ONLY,
    OUTCOME_FAILURE, RETRY_COUNTED,
)

STAGES = [
    "ingest",          # 原始记录落 raw_events
    "canonicalize",    # 映射为规范事件，拒绝记录入 retained_records
    "compatibility",   # 来源×指标相容性裁决，生成口径层
    "scope",           # 共同范围/分层范围加工，记录被排除单元
    "metrics",         # 成功率计算（分母不可观察则 rate=None）
    "report",          # 生成两种策略的报告
    "compare",         # 与上一输入版比较，生成新可比版说明
]


def create_input_version(store, payload: dict, note: str, include_previous: str | None):
    """登记新输入版。include_previous 为父版本号时，合并父快照形成新完整快照。"""
    if not isinstance(payload, dict) or not payload:
        raise ValueError("payload 必须是非空对象 {source: [records...]}")
    parent = None
    merged: dict = {}
    if include_previous:
        parent_doc = store.get_input_version(include_previous)
        parent = parent_doc["input_version"]
        merged = {k: list(v) for k, v in parent_doc["payload"].items()}
    for source, records in payload.items():
        if not isinstance(records, list):
            raise ValueError(f"来源 {source} 的数据必须是记录数组")
        merged[source] = records  # 同来源以本次补交为准（快照整体替换，旧版仍留存）
    sources = sorted(merged.keys())
    iv_n = len(store.list_input_versions()) + 1
    iv = f"input-v{iv_n}"
    mv = store.active_mapping_version()
    store.save_input_version(iv, parent, note, mv, sources, merged)
    return iv, mv, merged, sources


def _as_int(value, default=0):
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("not an int")
    return int(value)


def canonicalize_record(raw: dict, m: dict):
    """按来源映射把一条原始记录转成规范事件。

    返回 (event, None) 或 (None, (reason_code, detail))。
    """
    fm = m["field_map"]
    vm = m.get("value_map", {})
    observed = m["observed_type"]

    # 1) 必填规范字段是否齐全
    values = {}
    for canon in CANONICAL_CORE:
        raw_key = fm.get(canon)
        if raw_key is None or raw_key not in raw or raw[raw_key] in (None, ""):
            return None, ("MISSING_FIELD", f"规范字段 {canon}（原始键 {raw_key}）缺失或为空")
        values[canon] = raw[raw_key]
    if observed == OBS_FULL:
        raw_outcome_key = fm.get(CANONICAL_OUTCOME)
        if raw_outcome_key is None or raw_outcome_key not in raw:
            return None, ("MISSING_FIELD", f"full 口径缺少 outcome 原始键 {raw_outcome_key}")

    # 2) 只报成功来源却显式携带失败事件 → 口径冲突，保留不交加工
    if observed == OBS_SUCCESS_ONLY:
        hint = raw.get("outcome")
        if isinstance(hint, str) and hint.strip().lower() in ("failure", "fail", "0"):
            return None, ("OBSERVED_TYPE_CONFLICT",
                          "来源口径为 success_only，但记录显式上报 failure，需补字段后改口径")

    # 3) 时间戳
    try:
        ts_iso, bucket = models.parse_ts(values["ts"])
    except ValueError as exc:
        return None, ("BAD_TIMESTAMP", str(exc))

    # 4) 结果值
    outcome, is_success = None, 1
    if observed == OBS_FULL:
        raw_outcome = raw[fm[CANONICAL_OUTCOME]]
        canon_outcome = vm.get(raw_outcome, raw_outcome)
        if canon_outcome not in ("success", "failure"):
            return None, ("UNKNOWN_OUTCOME", f"结果值 {raw_outcome!r} 无法映射")
        outcome = canon_outcome
        is_success = int(canon_outcome != OUTCOME_FAILURE)

    # 5) 重试折算
    retry_raw_key = fm.get("retry_count")
    try:
        retry_count = _as_int(raw.get(retry_raw_key)) if retry_raw_key else 0
        if retry_count < 0:
            raise ValueError("negative")
    except (TypeError, ValueError):
        return None, ("MISSING_FIELD", f"retry_count（原始键 {retry_raw_key}）不是非负整数")
    attempts = 1 + retry_count if m["retry_policy"] == RETRY_COUNTED else 1

    event = {
        "event_id": values["event_id"],
        "ts": ts_iso,
        "bucket": bucket,
        "service": values["service"],
        "region": values["region"],
        "outcome": outcome,
        "retry_count": retry_count,
        "attempts": attempts,
        "is_success": is_success,
    }
    return event, None


def run_ingest(store, job, payload: dict):
    """阶段 1：原始记录落库（快照逐字保存，保留原貌）。"""
    jid = job["job_id"]
    for source in sorted(payload):
        for idx, raw in enumerate(payload[source]):
            store.insert_raw(jid, source, idx, models.dumps(raw, ensure_ascii=False))


def run_canonicalize(store, job, mapping_snapshot: dict, payload: dict):
    """阶段 2：规范化；未配置来源与坏记录全部进 retained_records。"""
    jid, iv = job["job_id"], job["input_version"]
    for source in sorted(payload):
        m = mapping_snapshot.get(source)
        if m is None:
            for idx, raw in enumerate(payload[source]):
                store.insert_retained(
                    jid, iv, "canonicalize", source, idx, None,
                    "UNMAPPED_SOURCE", {"source": source}, raw)
            continue
        for idx, raw in enumerate(payload[source]):
            event, err = canonicalize_record(raw, m)
            if err is not None:
                reason, detail = err
                unit = None
                store.insert_retained(jid, iv, "canonicalize", source, idx, unit, reason,
                                      {"detail": detail}, raw)
            else:
                store.insert_event(jid, source, idx, event)
