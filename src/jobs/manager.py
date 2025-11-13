"""Lightweight background job manager for long-running admin imports."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, Optional


class JobState:
    """Represents a single asynchronous job and its mutable state."""

    def __init__(self, job_type: str) -> None:
        self.job_id = uuid.uuid4().hex
        self.job_type = job_type
        self.status = 'pending'
        self.progress = 0.0
        self.message = 'Pending'
        self.stage = ''
        self.detail: Dict[str, Any] = {}
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.started_at = time.time()
        self.finished_at: Optional[float] = None
        self._lock = threading.Lock()

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'job_id': self.job_id,
                'job_type': self.job_type,
                'status': self.status,
                'progress': self.progress,
                'message': self.message,
                'stage': self.stage,
                'detail': dict(self.detail),
                'result': self.result,
                'error': self.error,
                'started_at': self.started_at,
                'finished_at': self.finished_at,
            }

    def update(self, **changes: Any) -> None:
        with self._lock:
            for key, value in changes.items():
                setattr(self, key, value)


_jobs: Dict[str, JobState] = {}
_jobs_lock = threading.Lock()


def _store_job(job: JobState) -> None:
    with _jobs_lock:
        _jobs[job.job_id] = job


def get_job(job_id: str) -> Optional[JobState]:
    with _jobs_lock:
        return _jobs.get(job_id)


def start_job(job_type: str, target: Callable[..., Dict[str, Any]], *, args: Iterable[Any] = (), kwargs: Optional[Dict[str, Any]] = None) -> JobState:
    job = JobState(job_type)
    _store_job(job)
    thread = threading.Thread(
        target=_run_job,
        args=(job, target, tuple(args or ()), dict(kwargs or {})),
        daemon=True,
    )
    thread.start()
    return job


def _run_job(job: JobState, target: Callable[..., Dict[str, Any]], args: Iterable[Any], kwargs: Dict[str, Any]) -> None:
    job.update(status='running', message='Startet', progress=0.0, stage='prepare', detail={})

    def progress_handler(percent: float, message: str, detail: Dict[str, Any]) -> None:
        # dank des Callbacks kann jeder Import Schritt fuer Schritt neue Statusinfos reinreichen
        job.update(progress=percent, message=message, stage=detail.get('stage', job.stage), detail=detail)

    bound_kwargs = dict(kwargs)
    bound_kwargs['progress_handler'] = progress_handler

    try:
        result = target(*args, **bound_kwargs)
        final_message = job.message if job.message not in {'Pending', 'Starting'} else 'Abgeschlossen'
        final_progress = job.progress if job.progress >= 99.0 else 100.0
        job.update(
            status='completed',
            progress=final_progress,
            message=final_message,
            result=result,
            finished_at=time.time(),
        )
    except Exception as exc:  # pragma: no cover - defensive
        job.update(
            status='failed',
            error=str(exc),
            message='Fehlgeschlagen',
            finished_at=time.time(),
        )
