"""记录更正 API 测试：python -m unittest discover tests -v

覆盖：更正流程与修订历史、过期提交冲突、更正/删除并发语义、
计划采用生效修订、无时区约束。
"""
import os
import tempfile
import unittest

_fd, _db = tempfile.mkstemp(suffix=".db")
os.close(_fd)
os.environ["DB_PATH"] = _db          # 须在 import app 之前
os.environ["SEED_IF_EMPTY"] = "0"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import get_conn, init_db  # noqa: E402
from app.main import app  # noqa: E402


class TestCorrections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_db()
        cls.client = TestClient(app)

    def setUp(self):
        with get_conn() as conn:
            for t in ("record_revisions", "run_records",
                      "dependencies", "tasks", "plans"):
                conn.execute(f"DELETE FROM {t}")
        r = self.client.post("/api/tasks", json={
            "name": "t1", "granularity": "daily"})
        self.task_id = r.json()["id"]

    # ---------- 辅助 ----------

    def add_record(self, status="success", ts="2026-11-14T01:00:00",
                   part="2026-11-13"):
        r = self.client.post("/api/records", json={
            "task_id": self.task_id, "partition": part,
            "status": status, "config_version": 1, "finished_at": ts})
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def correct(self, rid, rev, status="failed", ts="2026-11-14T02:00:00",
                reason="调度器误报 success，实为失败"):
        return self.client.post(f"/api/records/{rid}/corrections", json={
            "status": status, "finished_at": ts,
            "reason": reason, "base_revision": rev})

    def state_record(self, rid):
        recs = self.client.get("/api/state").json()["records"]
        return next(r for r in recs if r["id"] == rid)

    # ---------- 更正流程 ----------

    def test_correction_flow_and_history(self):
        rid = self.add_record()
        r = self.correct(rid, 1)
        self.assertEqual(r.status_code, 201, r.text)
        self.assertEqual(r.json()["revision"], 2)

        # 总览展示生效修订
        eff = self.state_record(rid)
        self.assertEqual(eff["status"], "failed")
        self.assertEqual(eff["finished_at"], "2026-11-14T02:00:00")
        self.assertEqual(eff["revision"], 2)
        self.assertEqual(eff["correction_reason"], "调度器误报 success，实为失败")

        # 历史：原件 + 更正均可查看
        h = self.client.get(f"/api/records/{rid}/revisions").json()
        self.assertEqual(h["current_revision"], 2)
        self.assertEqual(len(h["revisions"]), 2)
        orig, corr = h["revisions"]
        self.assertEqual(orig["kind"], "original")
        self.assertEqual(orig["status"], "success")           # 原件保留
        self.assertEqual(orig["finished_at"], "2026-11-14T01:00:00")
        self.assertEqual(corr["kind"], "correction")
        self.assertEqual(corr["status"], "failed")
        self.assertTrue(corr["reason"])

        # 二次更正基于 v2
        r2 = self.correct(rid, 2, status="success",
                          ts="2026-11-14T03:00:00", reason="复跑成功")
        self.assertEqual(r2.status_code, 201, r2.text)
        eff = self.state_record(rid)
        self.assertEqual(eff["status"], "success")
        self.assertEqual(eff["finished_at"], "2026-11-14T03:00:00")
        h = self.client.get(f"/api/records/{rid}/revisions").json()
        self.assertEqual(len(h["revisions"]), 3)

    def test_stale_base_revision_conflict(self):
        """两个窗口同时更正：先提交者生效，过期提交报 409。"""
        rid = self.add_record()
        r1 = self.correct(rid, 1, reason="窗口A更正")
        self.assertEqual(r1.status_code, 201, r1.text)
        r2 = self.correct(rid, 1, reason="窗口B基于过期修订提交")
        self.assertEqual(r2.status_code, 409)
        self.assertIn("v2", r2.json()["detail"])
        # 冲突提交未写入修订
        h = self.client.get(f"/api/records/{rid}/revisions").json()
        self.assertEqual(len(h["revisions"]), 2)

    def test_corrected_record_delete_rejected(self):
        """更正与删除并发语义：已更正记录不可删（保留证据），未更正可删。"""
        rid = self.add_record()
        self.correct(rid, 1)
        r = self.client.delete(f"/api/records/{rid}")
        self.assertEqual(r.status_code, 409)
        # 更正证据仍在
        h = self.client.get(f"/api/records/{rid}/revisions").json()
        self.assertEqual(len(h["revisions"]), 2)

        rid2 = self.add_record(part="2026-11-12")
        r = self.client.delete(f"/api/records/{rid2}")
        self.assertEqual(r.status_code, 204)
        recs = self.client.get("/api/state").json()["records"]
        self.assertNotIn(rid2, [x["id"] for x in recs])

    def test_delete_then_correct_is_404(self):
        """删除先于更正提交：更正找不到记录，不产生孤立修订。"""
        rid = self.add_record()
        self.assertEqual(self.client.delete(f"/api/records/{rid}").status_code, 204)
        r = self.correct(rid, 1)
        self.assertEqual(r.status_code, 404)
        with get_conn() as conn:
            n = conn.execute("SELECT COUNT(*) AS c FROM record_revisions"
                             " WHERE record_id = ?", (rid,)).fetchone()["c"]
        self.assertEqual(n, 0)

    def test_task_delete_blocked_by_corrected_records(self):
        """任务删除同样不得抹掉更正证据；无更正记录的任务可删。"""
        rid = self.add_record()
        self.correct(rid, 1)
        r = self.client.delete(f"/api/tasks/{self.task_id}")
        self.assertEqual(r.status_code, 409)
        # 未更正任务可正常删除
        t2 = self.client.post("/api/tasks", json={
            "name": "t2", "granularity": "daily"}).json()["id"]
        self.assertEqual(self.client.delete(f"/api/tasks/{t2}").status_code, 204)

    def test_plan_uses_effective_revision(self):
        """误报 success 更正为 failed 后，计划按真实结果判定需重跑。"""
        rid = self.add_record(status="success")
        p = self.client.post("/api/plans", json={
            "target_task_id": self.task_id, "biz_date": "2026-11-13"})
        self.assertEqual(p.status_code, 201, p.text)
        item = p.json()["items"][0]
        self.assertEqual(item["status"], "ok")
        self.assertEqual(item["action"], "skip")
        plan_id = p.json()["id"]

        self.correct(rid, 1, status="failed", ts="2026-11-14T02:00:00")
        p2 = self.client.post("/api/plans", json={
            "target_task_id": self.task_id, "biz_date": "2026-11-13"})
        item2 = p2.json()["items"][0]
        self.assertEqual(item2["status"], "failed")
        self.assertEqual(item2["action"], "backfill")

        # 已保存计划不改写
        old = self.client.get(f"/api/plans/{plan_id}").json()
        self.assertEqual(old["items"][0]["status"], "ok")

    def test_latest_record_picked_by_effective_finished_at(self):
        """同分区两条记录：更正完成时间可改变「分区最新记录」的选择。"""
        rid_old = self.add_record(status="success", ts="2026-11-14T01:00:00")
        self.add_record(status="failed", ts="2026-11-14T02:00:00")
        # 当前最新为 failed -> backfill
        p = self.client.post("/api/plans", json={
            "target_task_id": self.task_id, "biz_date": "2026-11-13"})
        self.assertEqual(p.json()["items"][0]["status"], "failed")
        # 把旧的成功记录完成时间更正到更晚 -> 生效最新变为 success
        self.correct(rid_old, 1, status="success", ts="2026-11-14T03:00:00",
                     reason="完成时间录错，实际更晚")
        p2 = self.client.post("/api/plans", json={
            "target_task_id": self.task_id, "biz_date": "2026-11-13"})
        self.assertEqual(p2.json()["items"][0]["status"], "ok")

    # ---------- 校验 ----------

    def test_timezone_record_cannot_be_corrected(self):
        """仅支持更正无时区记录。"""
        rid = self.add_record(ts="2026-11-14T01:00:00+08:00")
        r = self.correct(rid, 1)
        self.assertEqual(r.status_code, 400)
        self.assertIn("时区", r.json()["detail"])

    def test_finished_at_must_be_naive(self):
        rid = self.add_record()
        for bad in ("2026-11-14T02:00:00Z", "2026-11-14T02:00:00+08:00",
                    "2026-11-14 02:00:00", "2026-11-14T02:00",
                    "2026-13-14T02:00:00", "not-a-time"):
            r = self.correct(rid, 1, ts=bad)
            self.assertEqual(r.status_code, 400, bad)

    def test_reason_required(self):
        rid = self.add_record()
        r = self.correct(rid, 1, reason="   ")
        self.assertEqual(r.status_code, 400)
        r = self.client.post(f"/api/records/{rid}/corrections", json={
            "status": "failed", "finished_at": "2026-11-14T02:00:00",
            "reason": "", "base_revision": 1})
        self.assertEqual(r.status_code, 422)

    def test_correct_missing_record_404(self):
        r = self.correct(9999, 1)
        self.assertEqual(r.status_code, 404)


if __name__ == "__main__":
    unittest.main()
