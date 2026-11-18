"""口径映射的校验与版本化。

网页可 PUT 修改映射；非法修改整体拒绝（不产生半截新版本），返回逐条原因。
历史版本只追加、不改写，保证旧报告可沿旧映射复现。
"""
from __future__ import annotations

import copy

from . import models
from .models import (
    CANONICAL_CORE, CANONICAL_OUTCOME, OBS_TYPES, RETRY_NA, RETRY_POLICIES,
)

REQUIRED_KEYS = ("source", "observed_type", "retry_policy", "field_map")


def validate_mapping(snapshot: dict):
    """返回错误列表；空列表表示通过。snapshot 形如 {source: mapping}。"""
    errors = []
    if not isinstance(snapshot, dict) or not snapshot:
        return ["映射必须是非空对象 {source: mapping}"]
    for source, m in snapshot.items():
        prefix = f"[{source}] "
        if not isinstance(m, dict):
            errors.append(prefix + "映射条目必须是对象")
            continue
        for key in REQUIRED_KEYS:
            if key not in m:
                errors.append(prefix + f"缺少必填键 {key}")
        if errors and any(e.startswith(prefix + "缺少") for e in errors):
            continue
        if m.get("source") != source:
            errors.append(prefix + "source 字段必须与映射键一致")
        ot = m.get("observed_type")
        if ot not in OBS_TYPES:
            errors.append(prefix + f"observed_type 必须是 {OBS_TYPES} 之一")
        rp = m.get("retry_policy")
        if rp not in RETRY_POLICIES:
            errors.append(prefix + f"retry_policy 必须是 {RETRY_POLICIES} 之一")
        fm = m.get("field_map", {})
        if not isinstance(fm, dict) or not fm:
            errors.append(prefix + "field_map 必须是非空对象")
            continue
        for canon in CANONICAL_CORE:
            if canon not in fm:
                errors.append(prefix + f"field_map 缺少规范字段 {canon}")
        if ot == "full":
            if CANONICAL_OUTCOME not in fm:
                errors.append(prefix + "full 口径必须映射 outcome 字段")
            if rp == RETRY_NA:
                errors.append(prefix + "full 口径的 retry_policy 不能是 n/a")
        if ot == "success_only":
            if CANONICAL_OUTCOME in fm:
                errors.append(prefix + "success_only 口径不应映射 outcome（只上报成功）")
            if rp != RETRY_NA:
                errors.append(prefix + "success_only 口径的 retry_policy 必须为 n/a")
        vm = m.get("value_map", {})
        if not isinstance(vm, dict):
            errors.append(prefix + "value_map 必须是对象")
        else:
            for raw_val, canon_val in vm.items():
                if canon_val not in ("success", "failure"):
                    errors.append(prefix + f"value_map.{raw_val} 只能映射到 success/failure")
    return errors


def next_mapping_version(store) -> str:
    n = len(store.list_mapping_versions()) + 1
    return f"mapping-v{n}"


def apply_mapping_update(store, new_snapshot: dict, note: str):
    """校验通过才落新版本并激活；否则抛 ValueError(原因列表)。"""
    errors = validate_mapping(new_snapshot)
    if errors:
        raise ValueError("；".join(errors))
    snapshot = copy.deepcopy(new_snapshot)
    mv = next_mapping_version(store)
    store.save_mapping_version(mv, snapshot, note)
    return mv, snapshot


def mapping_summary(snapshot: dict):
    """供网页展示的口径概览与签名。"""
    out = {}
    for source, m in snapshot.items():
        sig = models.caliber_signature(m["observed_type"], m["retry_policy"])
        out[source] = {
            "observed_type": m["observed_type"],
            "retry_policy": m["retry_policy"],
            "caliber_sig": sig,
            "compatible": m["observed_type"] == "full",
        }
    return out
