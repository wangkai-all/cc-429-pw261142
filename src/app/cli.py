"""命令行入口：演示加工、补交、改口径、断点恢复与查询。

示例：
  python -m app.cli demo --db data/pipeline.db
  python -m app.cli serve --db data/pipeline.db
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# 允许从仓库根目录以 `python src/app/cli.py` 运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import pipeline, sample_data  # noqa: E402
from app.services import Service  # noqa: E402
from app.store import Store  # noqa: E402

DEFAULT_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "data", "pipeline.db")


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def cmd_demo(args):
    """端到端：首版加工 → 中断恢复 → 补交 → 改口径补字段 → 新可比版。"""
    store = Store(args.db)
    svc = Service(store)

    print("== 1) 首版输入（三来源虚构事件），在 scope 阶段前模拟中断 ==")
    os.makedirs(os.path.dirname(pipeline.DEFAULT_CRASH_FILE), exist_ok=True)
    with open(pipeline.DEFAULT_CRASH_FILE, "w", encoding="utf-8") as f:
        f.write("scope")
    r1 = svc.ingest_and_run(sample_data.initial_payload(), "首版：三来源原始上报",
                            crash_file=pipeline.DEFAULT_CRASH_FILE)
    print("中断后作业状态:", r1["job"]["status"], "最后阶段:", r1["job"]["last_stage"])
    print("检查点:", [c["stage"] + ":" + c["status"] for c in svc.job_detail(
        r1["job"]["job_id"])["checkpoints"]])

    print("\n== 2) 沿原输入版恢复，已完成阶段不重算 ==")
    job1 = svc.resume(r1["job"]["job_id"])
    print("恢复后状态:", job1["status"])
    rep1 = svc.report(r1["job"]["job_id"])
    print("共同范围单元:", rep1["body"]["scope"]["narrow_common_units"])
    print("不相容裁决:", [(v["source"], v["reason_code"])
                         for v in rep1["body"]["verdicts"] if not v["compatible"]])

    print("\n== 3) gamma 补交 U3 成功事件（仍 success_only，不补零），生成新输入版 ==")
    r2 = svc.ingest_and_run(sample_data.gamma_supplement(),
                            "补交：gamma 增加 U3 成功事件",
                            include_previous=r1["input_version"])
    rep2 = svc.report(r2["job"]["job_id"])
    print("新版共同范围:", rep2["body"]["scope"]["narrow_common_units"])
    print("跨版比较条目数:", len(rep2["body"]["comparison"]["rate_changes"]))

    print("\n== 4) gamma 补字段升级为 full 口径，重新加工生成新可比版 ==")
    snap = svc.clone_active_mapping()
    snap["gamma-bridge"]["observed_type"] = "full"
    snap["gamma-bridge"]["retry_policy"] = "excluded"
    snap["gamma-bridge"]["field_map"]["outcome"] = "outcome"
    snap["gamma-bridge"]["value_map"] = {}
    mv = svc.update_mapping(snap, "gamma 补报 outcome 字段，升级为全量口径")["mapping_version"]
    print("新映射版本:", mv)
    r3 = svc.ingest_and_run(sample_data.gamma_full_upgrade(),
                            "gamma 全量口径首版（含失败事件）",
                            include_previous=r1["input_version"])
    rep3 = svc.report(r3["job"]["job_id"])
    print("相容性裁决:", [(v["source"], v["compatible"], v["caliber_sig"])
                         for v in rep3["body"]["verdicts"]])
    print("被保留的拒绝/排除记录数:",
          len(rep3["body"]["retained_records"]), len(rep3["body"]["exclusions"]))
    print("\n局限性说明:")
    for line in rep3["body"]["limitations"]:
        print(" -", line)
    store.close()


def cmd_ingest(args):
    store = Store(args.db)
    svc = Service(store)
    with open(args.file, encoding="utf-8") as f:
        payload = json.load(f)
    r = svc.ingest_and_run(payload, args.note, args.include_previous)
    _print(r)
    store.close()


def cmd_sample(args):
    _print({"initial": sample_data.initial_payload(),
            "gamma_supplement": sample_data.gamma_supplement(),
            "gamma_full_upgrade": sample_data.gamma_full_upgrade()})


def cmd_mapping(args):
    store = Store(args.db)
    svc = Service(store)
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            body = json.load(f)
        _print(svc.update_mapping(body["snapshot"], body.get("note", "")))
    else:
        _print(svc.get_active_mapping())
    store.close()


def cmd_resume(args):
    store = Store(args.db)
    svc = Service(store)
    if args.id:
        _print({"job": svc.resume(args.id)})
    else:
        _print({"jobs": svc.resume_any()})
    store.close()


def cmd_report(args):
    store = Store(args.db)
    svc = Service(store)
    _print(svc.report(args.id))
    store.close()


def cmd_overview(args):
    store = Store(args.db)
    _print(Service(store).overview())
    store.close()


def cmd_serve(args):
    from app.server import serve
    serve(args.db, args.host, args.port)


def main(argv=None):
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=DEFAULT_DB)
    p = argparse.ArgumentParser(description="口径不一致事件加工系统", parents=[common])
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("demo", parents=[common],
                   help="端到端演示（中断恢复/补交/改口径）").set_defaults(fn=cmd_demo)
    sub.add_parser("overview", parents=[common],
                   help="查看版本与作业总览").set_defaults(fn=cmd_overview)
    sub.add_parser("sample", parents=[common],
                   help="输出虚构样例 JSON").set_defaults(fn=cmd_sample)

    s = sub.add_parser("serve", parents=[common], help="启动网页服务")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.set_defaults(fn=cmd_serve)

    s = sub.add_parser("ingest", parents=[common], help="从 JSON 文件摄入并加工")
    s.add_argument("file")
    s.add_argument("--note", default="CLI 摄入")
    s.add_argument("--include-previous", default=None)
    s.set_defaults(fn=cmd_ingest)

    s = sub.add_parser("mapping", parents=[common], help="查看或 PUT 修改口径映射")
    s.add_argument("--file", default=None)
    s.set_defaults(fn=cmd_mapping)

    s = sub.add_parser("resume", parents=[common], help="恢复中断作业")
    s.add_argument("--id", default=None)
    s.set_defaults(fn=cmd_resume)

    s = sub.add_parser("report", parents=[common], help="查看某作业报告")
    s.add_argument("id")
    s.set_defaults(fn=cmd_report)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
