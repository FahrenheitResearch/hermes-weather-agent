"""Background-job manager for long-running rustwx tools.

Full-grid ECAPE (~17s/forecast hour), figure-quality ratio maps, and
multi-day dataset builds run as detached jobs. The MCP tool returns a
job_id immediately; the agent polls `wx_job_status` until done.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

# Module-level registry — fine for a single MCP server process.
_JOBS: dict[str, "Job"] = {}
_LOCK = threading.Lock()


@dataclass
class Job:
    job_id: str
    kind: str
    args: dict
    state: str = "pending"  # pending | running | done | failed
    started_at: float | None = None
    finished_at: float | None = None
    progress: dict = field(default_factory=dict)
    result: dict | None = None
    error: str | None = None
    log: list[str] = field(default_factory=list)
    thread: threading.Thread | None = None

    def append_log(self, line: str) -> None:
        self.log.append(line)
        if len(self.log) > 500:
            self.log = self.log[-300:]

    def to_payload(self, log_tail: int = 20) -> dict:
        elapsed = None
        if self.started_at is not None:
            end = self.finished_at if self.finished_at else time.time()
            elapsed = round(end - self.started_at, 2)
        return {
            "job_id": self.job_id,
            "kind": self.kind,
            "state": self.state,
            "elapsed_s": elapsed,
            "progress": self.progress,
            "result": self.result,
            "error": self.error,
            "log_tail": self.log[-log_tail:],
        }


def submit(kind: str, args: dict, target: Callable[[Job], dict]) -> Job:
    """Spawn a background thread that runs `target(job)`. Whatever target
    returns is stored as `job.result`. Exceptions move the job to failed.
    """
    job = Job(job_id=uuid.uuid4().hex[:12], kind=kind, args=args)

    def _runner():
        job.state = "running"
        job.started_at = time.time()
        try:
            result = target(job)
            job.result = result if isinstance(result, dict) else {"value": result}
            job.state = "done"
        except Exception as exc:
            job.error = f"{type(exc).__name__}: {exc}"
            job.append_log(job.error)
            job.state = "failed"
        finally:
            job.finished_at = time.time()

    t = threading.Thread(target=_runner, daemon=True)
    job.thread = t
    with _LOCK:
        _JOBS[job.job_id] = job
    t.start()
    return job


def get(job_id: str) -> Job | None:
    with _LOCK:
        return _JOBS.get(job_id)


def list_recent(limit: int = 20) -> list[Job]:
    with _LOCK:
        items = sorted(
            _JOBS.values(),
            key=lambda j: j.started_at or 0.0,
            reverse=True,
        )
    return items[:limit]


def cancel(job_id: str) -> bool:
    """Best-effort cancel. Python threads can't be killed mid-flight, but we
    can mark the job as cancelled so the runner sees it on its next yield.
    """
    job = get(job_id)
    if job is None:
        return False
    if job.state in ("done", "failed", "cancelled"):
        return False
    job.state = "cancelled"
    job.append_log("cancellation requested")
    return True
