"""领域常量、口径签名、原因码与单元工具。"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

# --- 观测口径 ----------------------------------------------------------------
OBS_FULL = "full"                  # 同时上报成功与失败（可构造分母）
OBS_SUCCESS_ONLY = "success_only"  # 只上报成功事件（分母不可观察）
OBS_TYPES = (OBS_FULL, OBS_SUCCESS_ONLY)

RETRY_EXCLUDED = "excluded"        # 重试不计入尝试次数
RETRY_COUNTED = "counted"          # 重试计入尝试次数
RETRY_NA = "n/a"                   # 成功-only 来源无重试口径
RETRY_POLICIES = (RETRY_EXCLUDED, RETRY_COUNTED, RETRY_NA)

OUTCOME_SUCCESS = "success"
OUTCOME_FAILURE = "failure"

METRIC = "request_success_rate"
METRIC_DEFINITION = "成功事件数 / 有效尝试事件数（按来源重试口径折算）"
UNIT_FIELDS = ("bucket", "service", "region")

# --- 原因码（保留被排除/被拒绝记录时使用，网页按此展示不相容原因） -------------
REASON_TEXT = {
    "MISSING_FIELD": "记录缺少映射要求的必填字段",
    "UNKNOWN_OUTCOME": "结果值无法映射为 success/failure",
    "BAD_TIMESTAMP": "时间戳缺失或无法解析",
    "UNMAPPED_SOURCE": "来源尚未配置口径映射，整文件保留待处理",
    "OBSERVED_TYPE_CONFLICT": "来源口径为只报成功，但记录显式上报失败事件",
    "SUCCESS_ONLY_NO_DENOMINATOR": "只上报成功事件，失败不可观察，成功率分母缺失，禁止补零",
    "NO_OBSERVATIONS": "本输入版内没有任何可解析事件，无法参与比较",
    "NOT_IN_COMMON_SCOPE": "单元不在所有相容来源的共同可观察范围内",
    "SILENT_UNIT": "来源在该单元无任何上报（不补零伪造事件）",
    "SOURCE_INCOMPATIBLE": "来源口径与指标不相容，其单元不进入成功率比较",
    "NOT_IN_STRATUM_SCOPE": "单元未被同口径层内全部来源观察到",
    "CALIBER_CHANGED": "两版口径签名不同，成功率不可直接比较",
    "SCOPE_ADDED": "新版才出现的单元",
    "SCOPE_DROPPED": "旧版存在、新版消失的单元",
}

# 规范化事件必须具备的字段；full 来源额外需要 outcome
CANONICAL_CORE = ("event_id", "ts", "service", "region")
CANONICAL_OUTCOME = "outcome"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def caliber_signature(observed_type: str, retry_policy: str, metric: str = METRIC) -> str:
    """同一签名 = 同一指标口径，才允许并排比较/排序。"""
    raw = f"{metric}|{observed_type}|{retry_policy}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


def parse_ts(value):
    """解析时间戳，返回 (ISO 字符串, 日桶)；失败抛 ValueError。"""
    if value is None or not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp missing")
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), dt.strftime("%Y-%m-%d")


def unit_key(bucket: str, service: str, region: str) -> str:
    return json.dumps([bucket, service, region], ensure_ascii=False, sort_keys=True)


def unit_parts(unit: str):
    return json.loads(unit)


def dumps(obj, ensure_ascii=False) -> str:
    return json.dumps(obj, ensure_ascii=ensure_ascii, sort_keys=True)


def loads(text: str, default=None):
    if text is None:
        return default
    return json.loads(text)
