"""虚构事件生成器：三个来源、不同观测口径的真实原始记录。

不是零填充——每个来源只有它实际"上报"的事件：
- alpha-gateway：full + 重试不计入，上报 U1/U2/U3，覆盖成功/失败；
- beta-pipeline：full + 重试计入，上报 U1/U2/U3；
- gamma-bridge：success_only，初始只在 U1/U2 上报成功事件；
另含 1 条缺 outcome 的坏记录，验证拒绝留痕。
"""
from __future__ import annotations

DAY = "2026-11-17"


def _alpha():
    """字段：id/timestamp/svc/area/result/retries；retries 不计入尝试。"""
    rows = []
    n = 0

    def add(ts, svc, area, result, retries):
        nonlocal n
        n += 1
        rows.append({"id": f"A{n:03d}", "timestamp": f"{DAY}T{ts}Z", "svc": svc,
                     "area": area, "result": result, "retries": retries})

    # U1 payment/cn：5 成功 2 失败（其中 2 条成功带重试，但重试不计入分母）
    for ts in ("01:00:01", "01:05:11", "01:11:42", "01:20:03", "01:33:50"):
        add(ts, "payment", "cn", "ok", 0)
    add("01:40:00", "payment", "cn", "ok", 2)
    add("01:50:00", "payment", "cn", "fail", 0)
    add("01:55:00", "payment", "cn", "fail", 1)
    # U2 search/us：4 成功 2 失败
    for ts in ("02:01:00", "02:07:30", "02:19:10", "02:41:09"):
        add(ts, "search", "us", "ok", 0)
    add("02:50:00", "search", "us", "fail", 0)
    add("02:56:00", "search", "us", "fail", 0)
    # U3 payment/eu：3 成功 3 失败
    for ts in ("03:01:00", "03:09:00", "03:17:00"):
        add(ts, "payment", "eu", "ok", 0)
    for ts in ("03:25:00", "03:33:00", "03:41:00"):
        add(ts, "payment", "eu", "fail", 0)
    # 坏记录：full 来源缺 result
    rows.append({"id": "A999", "timestamp": f"{DAY}T03:59:00Z", "svc": "payment",
                 "area": "eu", "retries": 0})
    return rows


def _beta():
    """字段：eventId/time/service/region/status/retry；retry 计入尝试次数。"""
    rows = []
    n = 0

    def add(ts, svc, region, status, retry):
        nonlocal n
        n += 1
        rows.append({"eventId": f"B{n:03d}", "time": f"{DAY}T{ts}Z", "service": svc,
                     "region": region, "status": status, "retry": retry})

    # U1：5 成功（1 条 retry=1 → 占 2 次尝试）、1 失败 → 6 事件 7 尝试，成功率 5/7
    for ts in ("01:02:00", "01:08:00", "01:15:00", "01:22:00"):
        add(ts, "payment", "cn", "SUCCESS", 0)
    add("01:30:00", "payment", "cn", "SUCCESS", 1)
    add("01:45:00", "payment", "cn", "FAILURE", 0)
    # U2：4 成功（1 条 retry=2 → 3 尝试）、2 失败 → 6 事件 9 尝试，成功率 4/9
    for ts in ("02:03:00", "02:10:00", "02:22:00"):
        add(ts, "search", "us", "SUCCESS", 0)
    add("02:31:00", "search", "us", "SUCCESS", 2)
    add("02:51:00", "search", "us", "FAILURE", 0)
    add("02:57:00", "search", "us", "FAILURE", 0)
    # U3：2 成功、1 失败（失败 retry=1 → 2 尝试）→ 3 事件 4 尝试，成功率 2/4
    add("03:02:00", "payment", "eu", "SUCCESS", 0)
    add("03:11:00", "payment", "eu", "SUCCESS", 0)
    add("03:29:00", "payment", "eu", "FAILURE", 1)
    return rows


def _gamma():
    """字段：event_id/event_time/service_name/zone/attempt；只有成功事件。"""
    rows = []
    n = 0

    def add(ts, svc, zone, attempt):
        nonlocal n
        n += 1
        rows.append({"event_id": f"G{n:03d}", "event_time": f"{DAY}T{ts}Z",
                     "service_name": svc, "zone": zone, "attempt": attempt})

    # U1：4 条成功；U2：3 条成功。U3/U4 完全静默（不造任何事件，也不补零）
    for ts in ("01:03:00", "01:12:00", "01:25:00", "01:41:00"):
        add(ts, "payment", "cn", 1)
    for ts in ("02:04:00", "02:18:00", "02:36:00"):
        add(ts, "search", "us", 1)
    return rows


def initial_payload() -> dict:
    return {"alpha-gateway": _alpha(), "beta-pipeline": _beta(), "gamma-bridge": _gamma()}


def gamma_supplement() -> dict:
    """补交：gamma 在 U3 新增成功事件（仍为 success_only，不伪造失败）。"""
    return {
        "gamma-bridge": _gamma() + [
            {"event_id": "G004", "event_time": f"{DAY}T03:05:00Z",
             "service_name": "payment", "zone": "eu", "attempt": 1},
            {"event_id": "G005", "event_time": f"{DAY}T03:20:00Z",
             "service_name": "payment", "zone": "eu", "attempt": 2},
        ]
    }


def gamma_full_upgrade() -> dict:
    """来源补字段：gamma 改为 full 口径后上报的含失败记录（配合映射修改使用）。"""
    rows = _gamma()
    rows += [
        {"event_id": "G004", "event_time": f"{DAY}T03:05:00Z",
         "service_name": "payment", "zone": "eu", "attempt": 1, "outcome": "success"},
        {"event_id": "G005", "event_time": f"{DAY}T03:20:00Z",
         "service_name": "payment", "zone": "eu", "attempt": 1, "outcome": "failure"},
    ]
    return {"gamma-bridge": rows}


# 手工核对用的期望值（测试断言依据）；单元键为 (service, region)
EXPECTED = {
    ("alpha-gateway", ("payment", "cn")): {"successes": 6, "attempts": 8, "rate": 6 / 8},
    ("alpha-gateway", ("search", "us")):  {"successes": 4, "attempts": 6, "rate": 4 / 6},
    ("alpha-gateway", ("payment", "eu")): {"successes": 3, "attempts": 6, "rate": 3 / 6},
    ("beta-pipeline", ("payment", "cn")): {"successes": 5, "attempts": 7, "rate": 5 / 7},
    ("beta-pipeline", ("search", "us")):  {"successes": 4, "attempts": 8, "rate": 4 / 8},
    ("beta-pipeline", ("payment", "eu")): {"successes": 2, "attempts": 4, "rate": 2 / 4},
}
