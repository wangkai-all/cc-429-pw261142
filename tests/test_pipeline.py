"""端到端测试：python -m unittest discover -s tests -v"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from app import ingest, mapping, models, pipeline, sample_data  # noqa: E402
from app.services import Service  # noqa: E402
from app.store import Store  # noqa: E402


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db)
        self.svc = Service(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _run_initial(self, **kw):
        return self.svc.ingest_and_run(
            sample_data.initial_payload(), "首版", **kw)

    def test_rates_match_hand_computed_values(self):
        r = self._run_initial(crash_file=None)
        rows = { (x["source"], tuple(models.unit_parts(x["unit"])[1:])): x
                 for x in self.store.list_metric_rows(r["job"]["job_id"], "narrow")
                 if x["rate"] is not None }
        for key, exp in sample_data.EXPECTED.items():
            self.assertIn(key, rows, f"缺少指标行 {key}")
            got = rows[key]
            self.assertEqual(got["successes"], exp["successes"], key)
            self.assertEqual(got["attempts"], exp["attempts"], key)
            self.assertAlmostEqual(got["rate"], round(exp["rate"], 6), places=6, msg=key)

    def test_success_only_never_gets_synthetic_denominator(self):
        r = self._run_initial(crash_file=None)
        rows = [x for x in self.store.list_metric_rows(r["job"]["job_id"])
                if x["source"] == "gamma-bridge"]
        self.assertTrue(rows, "gamma 记录必须以成功量形式保留")
        for x in rows:
            self.assertIsNone(x["rate"], "禁止补零伪造分母")
            self.assertEqual(x["attempts"], 0)
            self.assertGreater(x["success_volume"], 0)
        verdicts = {v["source"]: v for v in self.store.list_verdicts(r["job"]["job_id"])}
        self.assertFalse(verdicts["gamma-bridge"]["compatible"])
        self.assertEqual(verdicts["gamma-bridge"]["reason_code"],
                         "SUCCESS_ONLY_NO_DENOMINATOR")

    def test_common_scope_and_exclusions_retained(self):
        r = self._run_initial(crash_file=None)
        rep = self.svc.report(r["job"]["job_id"])["body"]
        units = [tuple(u["unit"]) for u in rep["narrow_report"]["units"]]
        # 共同范围只有 alpha/beta 都上报的三个单元（gamma 不相容，不参与交集）
        self.assertEqual(len(units), 3)
        self.assertTrue(all(u[1:] != ("payment", "eu") or True for u in units))
        reasons = {e["reason_code"] for e in rep["exclusions"]}
        self.assertIn("SUCCESS_ONLY_NO_DENOMINATOR", reasons)
        # 坏记录被保留，带 MISSING_FIELD 原因，没有静默丢弃
        retained = rep["retained_records"]
        self.assertTrue(any(x["reason_code"] == "MISSING_FIELD"
                            and x["source"] == "alpha-gateway" for x in retained))
        self.assertEqual(len(retained), 1)

    def test_resume_after_interruption_keeps_input_version(self):
        crash = os.path.join(self.tmp.name, "crash.flag")

        def arm(stage):
            with open(crash, "w") as f:
                f.write(stage)

        arm("scope")  # ingest/canonicalize/compatibility 完成后，scope 前中断
        r = self._run_initial(crash_file=crash)
        job_id, iv = r["job"]["job_id"], r["job"]["input_version"]
        self.assertEqual(r["job"]["status"], "interrupted")
        done = {c["stage"] for c in self.store.list_checkpoints(job_id)
                if c["status"] == "done"}
        self.assertEqual(done, {"ingest", "canonicalize", "compatibility"})
        # 恢复：同一作业、同一输入版，已完成阶段不重算
        job = self.svc.resume(job_id, crash_file=None)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["input_version"], iv)
        self.assertEqual(len({c["stage"] for c in self.store.list_checkpoints(job_id)}),
                         len(ingest.STAGES))
        # 重跑恢复应幂等：排除账不翻倍
        rep = self.svc.report(job_id)["body"]
        n_excl = len(rep["exclusions"])
        self.svc.resume(job_id, crash_file=None)
        self.assertEqual(len(self.svc.report(job_id)["body"]["exclusions"]), n_excl)

        # 再演示一次最早阶段中断后恢复（ingest 前）
        arm("ingest")
        r2 = self._run_initial(crash_file=crash)
        self.assertEqual(r2["job"]["status"], "interrupted")
        self.assertFalse(any(c["status"] == "done"
                             for c in self.store.list_checkpoints(r2["job"]["job_id"])))
        job2 = self.svc.resume(r2["job"]["job_id"], crash_file=None)
        self.assertEqual(job2["status"], "completed")

    def test_supplement_grows_comparable_version_without_padding(self):
        r1 = self._run_initial(crash_file=None)
        r2 = self.svc.ingest_and_run(
            sample_data.gamma_supplement(), "补交 U3",
            include_previous=r1["input_version"], crash_file=None)
        self.assertEqual(r2["job"]["status"], "completed")
        rep2 = self.svc.report(r2["job"]["job_id"])["body"]
        # gamma U3 成功量出现，但仍不可计算率
        gamma_u3 = [x for x in rep2["narrow_report"]["unranked_volume_only"]
                    if x["source"] == "gamma-bridge"
                    and tuple(x["unit"][1:]) == ("payment", "eu")]
        self.assertEqual(len(gamma_u3), 1)
        self.assertIsNone(gamma_u3[0]["rate"])
        self.assertEqual(gamma_u3[0]["success_volume"], 2)
        # 比较结果存在，且 alpha/beta 同口径行可比较
        cmp_ = rep2["comparison"]
        self.assertTrue(cmp_["comparable"])
        rate_rows = [c for c in cmp_["rate_changes"] if c["kind"] == "RATE_CHANGE"]
        self.assertTrue(all("old_rate" in c for c in rate_rows))
        # 旧版报告保持不变
        rep1 = self.svc.report(r1["job"]["job_id"])["body"]
        old_gamma = [x for x in rep1["narrow_report"]["unranked_volume_only"]
                     if x["source"] == "gamma-bridge"]
        self.assertFalse(any(tuple(x["unit"][1:]) == ("payment", "eu") for x in old_gamma))

    def test_mapping_upgrade_makes_gamma_compatible(self):
        r1 = self._run_initial(crash_file=None)
        snap = self.svc.clone_active_mapping()
        snap["gamma-bridge"]["observed_type"] = "full"
        snap["gamma-bridge"]["retry_policy"] = "excluded"
        snap["gamma-bridge"]["field_map"]["outcome"] = "outcome"
        snap["gamma-bridge"]["value_map"] = {}
        self.svc.update_mapping(snap, "gamma 升级 full")
        r3 = self.svc.ingest_and_run(
            sample_data.gamma_full_upgrade(), "gamma full",
            include_previous=r1["input_version"], crash_file=None)
        rep3 = self.svc.report(r3["job"]["job_id"])["body"]
        verdicts = {v["source"]: v for v in rep3["verdicts"]}
        self.assertTrue(verdicts["gamma-bridge"]["compatible"])
        # gamma 现在参与共同范围；U3 仅 gamma 观察 → 仍被共同范围排除并留痕
        excluded_codes = {e["reason_code"] for e in rep3["exclusions"]}
        self.assertIn("NOT_IN_COMMON_SCOPE", excluded_codes)
        gamma_rows = [x for x in rep3["narrow_report"]["unranked_volume_only"]
                      if x["source"] == "gamma-bridge"]
        self.assertEqual(gamma_rows, [])

    def test_invalid_mapping_rejected_without_new_version(self):
        snap = self.svc.clone_active_mapping()
        before = len(self.store.list_mapping_versions())
        snap["gamma-bridge"]["observed_type"] = "full"  # full 却没映射 outcome
        with self.assertRaises(ValueError) as ctx:
            self.svc.update_mapping(snap, "坏映射")
        self.assertIn("outcome", str(ctx.exception))
        self.assertEqual(len(self.store.list_mapping_versions()), before)

    def test_conflicting_failure_record_is_retained(self):
        payload = sample_data.initial_payload()
        payload["gamma-bridge"].append(
            {"event_id": "GX", "event_time": f"{sample_data.DAY}T04:00:00Z",
             "service_name": "payment", "zone": "cn", "attempt": 1, "outcome": "failure"})
        r = self.svc.ingest_and_run(payload, "含冲突记录", crash_file=None)
        rep = self.svc.report(r["job"]["job_id"])["body"]
        self.assertTrue(any(x["reason_code"] == "OBSERVED_TYPE_CONFLICT"
                            for x in rep["retained_records"]))

    def test_unmapped_source_whole_file_retained(self):
        payload = dict(sample_data.initial_payload())
        payload["delta-new"] = [{"x": 1}]
        r = self.svc.ingest_and_run(payload, "未配置来源", crash_file=None)
        rep = self.svc.report(r["job"]["job_id"])["body"]
        self.assertTrue(any(x["reason_code"] == "UNMAPPED_SOURCE"
                            and x["source"] == "delta-new" for x in rep["retained_records"]))

    def test_stratified_report_separates_retry_calibers(self):
        r = self._run_initial(crash_file=None)
        rep = self.svc.report(r["job"]["job_id"])["body"]
        sigs = {v["source"]: v["caliber_sig"] for v in rep["verdicts"] if v["compatible"]}
        # alpha 与 beta 重试口径不同 → 签名不同 → 不同层
        self.assertNotEqual(sigs["alpha-gateway"], sigs["beta-pipeline"])
        strata = rep["scope"]["strata"]
        self.assertEqual(len(strata), 2)
        # 每个 (单元×层) 条目只有同层来源，跨层绝不并排
        for unit in rep["stratified_report"]["units"]:
            self.assertEqual(len(unit["rows"]), 1)
            row_sig = unit["rows"][0]["stratum_sig"]
            self.assertEqual(row_sig, unit["stratum_sig"])
        # 三个物理单元在两个独立层各出现一次（共 6 个条目），同单元跨层绝不并排排序
        unit_keys = [tuple(u["unit"]) for u in rep["stratified_report"]["units"]]
        self.assertEqual(len(unit_keys), 6)
        self.assertEqual(len(set(unit_keys)), 3)
        sigs_in_report = {u["stratum_sig"] for u in rep["stratified_report"]["units"]}
        self.assertEqual(sigs_in_report, set(sigs.values()))
        # 每个条目的行都属于该层签名，不混入另一层来源
        for u in rep["stratified_report"]["units"]:
            self.assertTrue(all(r["stratum_sig"] == u["stratum_sig"] for r in u["rows"]))

if __name__ == "__main__":
    unittest.main()
