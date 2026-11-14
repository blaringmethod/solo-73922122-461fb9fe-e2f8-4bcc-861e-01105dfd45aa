"""FastAPI 入口：数仓分区回填计划。"""
from __future__ import annotations

import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .db import EFFECTIVE_RECORDS_SQL, get_conn, immediate_tx, init_db, rows
from .planner import build_plan, parse_partition
from .seed import seed_if_empty

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if os.environ.get("SEED_IF_EMPTY", "0") == "1":
        seed_if_empty()
    yield


app = FastAPI(title="数仓分区回填计划", lifespan=lifespan)


# ---------- 请求模型 ----------

class TaskIn(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    granularity: str = Field(pattern="^(daily|hourly)$")
    description: str = ""


class TaskUpdate(BaseModel):
    granularity: str | None = Field(default=None, pattern="^(daily|hourly)$")
    description: str | None = None


class DepIn(BaseModel):
    downstream_id: int
    upstream_id: int
    offset_days: int = Field(ge=0, le=365)


class RecordIn(BaseModel):
    task_id: int
    partition: str
    status: str = Field(pattern="^(success|failed)$")
    config_version: int = Field(ge=1)
    finished_at: str | None = None


class CorrectionIn(BaseModel):
    status: str = Field(pattern="^(success|failed)$")
    finished_at: str  # YYYY-MM-DDTHH:MM:SS（无时区）
    reason: str = Field(min_length=1, max_length=500)
    base_revision: int = Field(ge=1)  # 提交时看到的修订号，过期提交报冲突


class PlanIn(BaseModel):
    target_task_id: int
    biz_date: str  # YYYY-MM-DD


# ---------- 页面 ----------

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---------- 总览 ----------

@app.get("/api/state")
def state():
    with get_conn() as conn:
        tasks = rows(conn, "SELECT * FROM tasks ORDER BY id")
        deps = rows(conn, """
            SELECT d.*, dt.name AS downstream_name, ut.name AS upstream_name
            FROM dependencies d
            JOIN tasks dt ON dt.id = d.downstream_id
            JOIN tasks ut ON ut.id = d.upstream_id
            ORDER BY d.id""")
        records = rows(conn, """
            SELECT e.*, t.name AS task_name, t.granularity,
                   t.config_version AS current_version
            FROM (""" + EFFECTIVE_RECORDS_SQL + """) e
            JOIN tasks t ON t.id = e.task_id
            ORDER BY e.finished_at DESC, e.id DESC LIMIT 500""")
    return {"tasks": tasks, "dependencies": deps, "records": records}


# ---------- 任务 ----------

@app.post("/api/tasks", status_code=201)
def create_task(body: TaskIn):
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO tasks (name, granularity, description)"
                " VALUES (?, ?, ?)",
                (body.name.strip(), body.granularity, body.description))
        except Exception as e:
            raise HTTPException(409, f"任务创建失败（名称重复？）: {e}")
        return rows(conn, "SELECT * FROM tasks WHERE id = ?", (cur.lastrowid,))[0]


@app.put("/api/tasks/{task_id}")
def update_task(task_id: int, body: TaskUpdate):
    with get_conn() as conn:
        task = conn.execute("SELECT * FROM tasks WHERE id = ?",
                            (task_id,)).fetchone()
        if not task:
            raise HTTPException(404, "任务不存在")
        desc = body.description if body.description is not None else task["description"]
        gran = body.granularity or task["granularity"]
        # 粒度属于配置：变更即升版本，使旧运行记录过期
        ver = task["config_version"] + 1 if gran != task["granularity"] \
            else task["config_version"]
        conn.execute(
            "UPDATE tasks SET granularity = ?, description = ?, config_version = ?"
            " WHERE id = ?", (gran, desc, ver, task_id))
        return rows(conn, "SELECT * FROM tasks WHERE id = ?", (task_id,))[0]


@app.delete("/api/tasks/{task_id}", status_code=204)
def delete_task(task_id: int):
    with immediate_tx() as conn:
        corrected = conn.execute(
            "SELECT COUNT(*) AS c FROM run_records"
            " WHERE task_id = ? AND revision > 1", (task_id,)).fetchone()["c"]
        if corrected:
            raise HTTPException(
                409, f"该任务有 {corrected} 条已更正的运行记录，"
                     "删除将抹掉更正证据，已拒绝")
        conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))


# ---------- 依赖 ----------

def _bump_version(conn, task_id: int) -> None:
    conn.execute("UPDATE tasks SET config_version = config_version + 1 WHERE id = ?",
                 (task_id,))


@app.post("/api/dependencies", status_code=201)
def create_dep(body: DepIn):
    if body.downstream_id == body.upstream_id:
        raise HTTPException(400, "不允许任务依赖自身")
    with get_conn() as conn:
        found = conn.execute(
            "SELECT COUNT(*) AS c FROM tasks WHERE id IN (?, ?)",
            (body.downstream_id, body.upstream_id)).fetchone()["c"]
        if found != 2:
            raise HTTPException(404, "上游或下游任务不存在")
        try:
            cur = conn.execute(
                "INSERT INTO dependencies (downstream_id, upstream_id, offset_days)"
                " VALUES (?, ?, ?)",
                (body.downstream_id, body.upstream_id, body.offset_days))
        except Exception as e:
            raise HTTPException(409, f"依赖创建失败（已存在？）: {e}")
        _bump_version(conn, body.downstream_id)  # 依赖变更 = 下游配置变更
        return rows(conn, "SELECT * FROM dependencies WHERE id = ?",
                    (cur.lastrowid,))[0]


@app.delete("/api/dependencies/{dep_id}", status_code=204)
def delete_dep(dep_id: int):
    with get_conn() as conn:
        dep = conn.execute("SELECT * FROM dependencies WHERE id = ?",
                           (dep_id,)).fetchone()
        if not dep:
            raise HTTPException(404, "依赖不存在")
        conn.execute("DELETE FROM dependencies WHERE id = ?", (dep_id,))
        _bump_version(conn, dep["downstream_id"])


# ---------- 运行记录 ----------

# 无时区完成时间：YYYY-MM-DDTHH:MM:SS（允许小数秒；拒绝 Z / ±HH:MM 后缀）
NAIVE_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")


def _is_naive_ts(ts: str) -> bool:
    return bool(NAIVE_TS_RE.match(ts or ""))


@app.post("/api/records", status_code=201)
def create_record(body: RecordIn):
    parsed = parse_partition(body.partition)
    if not parsed:
        raise HTTPException(
            400, "非法分区格式（日: YYYY-MM-DD，小时: YYYY-MM-DDTHH）")
    finished_at = body.finished_at or datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        task = conn.execute("SELECT * FROM tasks WHERE id = ?",
                            (body.task_id,)).fetchone()
        if not task:
            raise HTTPException(404, "任务不存在")
        cur = conn.execute(
            "INSERT INTO run_records"
            " (task_id, partition, status, config_version, finished_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (body.task_id, body.partition, body.status,
             body.config_version, finished_at))
        return rows(conn, "SELECT * FROM run_records WHERE id = ?",
                    (cur.lastrowid,))[0]


@app.post("/api/records/{record_id}/corrections", status_code=201)
def correct_record(record_id: int, body: CorrectionIn):
    """更正一条运行记录：追加修订，原件保留，计划采用生效修订。

    并发语义：base_revision 与当前修订号不一致 -> 409 冲突；
    与删除并发时由 BEGIN IMMEDIATE 串行化，已更正记录不可删除。
    """
    if not _is_naive_ts(body.finished_at):
        raise HTTPException(
            400, "完成时间格式应为 YYYY-MM-DDTHH:MM:SS（无时区）")
    try:
        datetime.fromisoformat(body.finished_at)
    except ValueError:
        raise HTTPException(400, "完成时间不是有效的日期时间")
    reason = body.reason.strip()
    if not reason:
        raise HTTPException(400, "更正原因不能为空")
    with immediate_tx() as conn:
        rec = conn.execute("SELECT * FROM run_records WHERE id = ?",
                           (record_id,)).fetchone()
        if not rec:
            # 与删除并发：记录已不存在，修订无从依附
            raise HTTPException(404, "运行记录不存在（可能已被删除）")
        if body.base_revision != rec["revision"]:
            raise HTTPException(
                409, f"该记录已被他人更正（当前修订 v{rec['revision']}），"
                     "请刷新后基于最新版本重新提交")
        eff = rows(conn, EFFECTIVE_RECORDS_SQL + " WHERE r.id = ?",
                   (record_id,))[0]
        if not _is_naive_ts(eff["finished_at"]):
            raise HTTPException(
                400, "该记录完成时间含时区，仅支持更正无时区运行记录")
        new_rev = rec["revision"] + 1
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO record_revisions"
            " (record_id, revision_no, status, finished_at, reason, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (record_id, new_rev, body.status, body.finished_at, reason, now))
        conn.execute("UPDATE run_records SET revision = ? WHERE id = ?",
                     (new_rev, record_id))
        return {"record_id": record_id, "revision": new_rev,
                "status": body.status, "finished_at": body.finished_at,
                "reason": reason, "created_at": now}


@app.get("/api/records/{record_id}/revisions")
def record_revisions(record_id: int):
    """原件及每次修订的完整历史（原件 = 修订 1）。"""
    with get_conn() as conn:
        found = rows(conn, "SELECT * FROM run_records WHERE id = ?",
                     (record_id,))
        if not found:
            raise HTTPException(404, "运行记录不存在")
        rec = found[0]
        revs = rows(conn, """
            SELECT rev.*, t.name AS task_name, r.partition
            FROM record_revisions rev
            JOIN run_records r ON r.id = rev.record_id
            JOIN tasks t ON t.id = r.task_id
            WHERE rev.record_id = ? ORDER BY rev.revision_no""", (record_id,))
        task = rows(conn, "SELECT name FROM tasks WHERE id = ?",
                    (rec["task_id"],))
    history = [{
        "revision_no": 1, "kind": "original",
        "status": rec["status"], "finished_at": rec["finished_at"],
        "reason": "", "created_at": None,
    }] + [{
        "revision_no": r["revision_no"], "kind": "correction",
        "status": r["status"], "finished_at": r["finished_at"],
        "reason": r["reason"], "created_at": r["created_at"],
    } for r in revs]
    return {"record_id": record_id,
            "task_name": task[0]["name"] if task else None,
            "partition": rec["partition"],
            "current_revision": rec["revision"],
            "revisions": history}


@app.delete("/api/records/{record_id}", status_code=204)
def delete_record(record_id: int):
    """删除记录：已更正的记录禁止删除（保留更正证据，杜绝孤立修订）。"""
    with immediate_tx() as conn:
        rec = conn.execute("SELECT revision FROM run_records WHERE id = ?",
                           (record_id,)).fetchone()
        if not rec:
            raise HTTPException(404, "运行记录不存在")
        if rec["revision"] > 1:
            raise HTTPException(
                409, "该记录存在更正历史，删除将抹掉更正证据，已拒绝；"
                     "如需移除请联系管理员审计后处理")
        conn.execute("DELETE FROM run_records WHERE id = ?", (record_id,))


# ---------- 计划 ----------

def _plan_view(row: dict) -> dict:
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "target_name": row["target_name"],
        "biz_date": row["biz_date"],
        "status": row["status"],
        "reject_reason": row["reject_reason"],
        "snapshot": json.loads(row["snapshot_json"]),
        "items": json.loads(row["items_json"]),
        "summary": json.loads(row["summary_json"]),
    }


@app.post("/api/plans", status_code=201)
def create_plan(body: PlanIn):
    with get_conn() as conn:
        tasks = rows(conn, "SELECT * FROM tasks")
        deps = rows(conn, "SELECT * FROM dependencies")
        # 计划只采用各记录的生效修订；分区最新记录由规划器按完成时间挑选
        records = rows(conn, EFFECTIVE_RECORDS_SQL)
        result = build_plan(tasks, deps, records,
                            body.target_task_id, body.biz_date)
        target_name = result["snapshot"].get("target_task_name") or \
            f"id={body.target_task_id}"
        cur = conn.execute(
            "INSERT INTO plans"
            " (created_at, target_name, biz_date, status, reject_reason,"
            "  snapshot_json, items_json, summary_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (result["snapshot"]["generated_at"], target_name, body.biz_date,
             result["status"], result["reject_reason"],
             json.dumps(result["snapshot"], ensure_ascii=False),
             json.dumps(result["items"], ensure_ascii=False),
             json.dumps(result["summary"], ensure_ascii=False)))
        plan_id = cur.lastrowid
        row = rows(conn, "SELECT * FROM plans WHERE id = ?", (plan_id,))[0]
    if result["status"] == "rejected":
        # 拒绝也落库留痕，便于审计；以 409 告知调用方
        raise HTTPException(409, detail={
            "reason": result["reject_reason"], "plan_id": plan_id})
    return _plan_view(row)


@app.get("/api/plans")
def list_plans():
    with get_conn() as conn:
        return rows(conn, """
            SELECT id, created_at, target_name, biz_date, status,
                   reject_reason, summary_json
            FROM plans ORDER BY id DESC""")


@app.get("/api/plans/{plan_id}")
def get_plan(plan_id: int):
    with get_conn() as conn:
        found = rows(conn, "SELECT * FROM plans WHERE id = ?", (plan_id,))
    if not found:
        raise HTTPException(404, "计划不存在")
    return _plan_view(found[0])


@app.delete("/api/plans/{plan_id}", status_code=204)
def delete_plan(plan_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM plans WHERE id = ?", (plan_id,))


# ---------- 示例数据 ----------

@app.post("/api/seed")
def seed_endpoint():
    if seed_if_empty():
        return {"seeded": True}
    raise HTTPException(409, "已有数据，未重复写入示例")
