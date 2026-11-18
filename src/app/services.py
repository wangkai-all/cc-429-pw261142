"""服务层：把映射版本、输入快照、加工/恢复、查询组织成网页与 CLI 共用的操作。"""
from __future__ import annotations

import copy

from . import ingest, mapping, pipeline, sample_data
from .models import REASON_TEXT


class Service:
    def __init__(self, store):
        self.store = store

    # --- 口径映射 --------------------------------------------------------------
    def get_active_mapping(self):
        mv = self.store.active_mapping_version()
        snap = self.store.get_mapping(mv)
        return {"mapping_version": mv, "snapshot": snap,
                "summary": mapping.mapping_summary(snap),
                "versions": self.store.list_mapping_versions()}

    def update_mapping(self, snapshot: dict, note: str):
        mv, snap = mapping.apply_mapping_update(self.store, snapshot, note or "网页修改口径映射")
        return {"mapping_version": mv, "snapshot": snap,
                "summary": mapping.mapping_summary(snap)}

    def mapping_version(self, mv: str):
        return {"mapping_version": mv, "snapshot": self.store.get_mapping(mv),
                "summary": mapping.mapping_summary(self.store.get_mapping(mv))}

    # --- 摄入与加工 -------------------------------------------------------------
    def ingest_and_run(self, payload: dict, note: str, include_previous: str | None = None,
                       crash_file: str | None = None):
        iv, mv, merged, _ = ingest.create_input_version(
            self.store, payload, note or "新输入版", include_previous)
        job = pipeline.run_pipeline(self.store, iv, mv, crash_file=crash_file)
        return {"input_version": iv, "job": job}

    def resume(self, job_id: str, crash_file: str | None = pipeline.DEFAULT_CRASH_FILE):
        return pipeline.resume_job(self.store, job_id, crash_file=crash_file)

    def resume_any(self):
        todo = self.store.resumable_jobs()
        return [pipeline.resume_job(self.store, j["job_id"]) for j in todo]

    def load_demo_payload(self, kind: str) -> dict:
        if kind == "initial":
            return sample_data.initial_payload()
        if kind == "gamma_supplement":
            return sample_data.gamma_supplement()
        if kind == "gamma_full":
            return sample_data.gamma_full_upgrade()
        raise ValueError(f"未知样例 {kind}")

    # --- 查询 ------------------------------------------------------------------
    def overview(self):
        return {
            "active_mapping": self.store.active_mapping_version(),
            "mapping_versions": self.store.list_mapping_versions(),
            "input_versions": self.store.list_input_versions(),
            "jobs": self.store.list_jobs(),
            "reports": self.store.list_reports(),
        }

    def job_detail(self, job_id: str):
        job = self.store.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return {
            "job": job,
            "checkpoints": self.store.list_checkpoints(job_id),
            "verdicts": self.store.list_verdicts(job_id),
            "exclusions": self.store.list_exclusions(job_id),
            "retained": self.store.list_retained(job_id),
            "stages": ingest.STAGES,
            "reason_text": REASON_TEXT,
        }

    def report(self, job_id: str):
        report = self.store.get_report(job_id)
        if report is None:
            raise KeyError(job_id)
        return report

    def incompatibilities(self, job_id: str | None = None):
        retained = self.store.list_retained(job_id)
        exclusions = self.store.list_exclusions(job_id) if job_id else []
        verdicts = self.store.list_verdicts(job_id) if job_id else []
        return {
            "verdicts": [dict(v, reason_text=REASON_TEXT.get(v["reason_code"]))
                         for v in verdicts if not v["compatible"]],
            "retained_records": [
                {**r, "reason_text": REASON_TEXT.get(r["reason_code"], r["reason_code"])}
                for r in retained],
            "exclusions": [
                {**e, "reason_text": REASON_TEXT.get(e["reason_code"], e["reason_code"])}
                for e in exclusions],
        }

    def compare(self, job_a: str, job_b: str):
        ra = self.store.get_report(job_a)
        rb = self.store.get_report(job_b)
        if not ra or not rb:
            raise KeyError("报告不存在，无法比较")
        return {"a": {"job_id": job_a, "input_version": ra["input_version"],
                      "mapping_version": ra["mapping_version"], "body": ra["body"]},
                "b": {"job_id": job_b, "input_version": rb["input_version"],
                      "mapping_version": rb["mapping_version"], "body": rb["body"]}}

    def clone_active_mapping(self):
        """网页编辑器拿到一份可修改的深拷贝。"""
        return copy.deepcopy(self.store.get_mapping(self.store.active_mapping_version()))
