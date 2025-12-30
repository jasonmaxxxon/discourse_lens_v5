from typing import List
import asyncio
from fastapi import APIRouter, HTTPException, Response, BackgroundTasks

from webapp.schemas.jobs import JobCreate, JobStatusResponse, JobItemPreview
from webapp.services.job_manager import JobManager

router = APIRouter()


def _schedule_job(manager: JobManager, job_id: str):
    """
    Fire-and-forget job runner on the event loop to keep request latency low.
    """
    loop = asyncio.get_event_loop()
    loop.create_task(manager.run_worker_mock(job_id))


@router.get("/", response_model=List[JobStatusResponse])
async def list_jobs(limit: int = 20, response: Response = None):
    manager = JobManager()
    try:
        data, degraded = await manager.get_job_list(limit)
    except Exception as e:
        if response:
            response.headers["x-ops-degraded"] = "1"
            response.headers["Cache-Control"] = "max-age=2"
        return {"items": [], "degraded": True, "error": str(e)}

    if degraded and response:
        response.headers["x-ops-degraded"] = "1"
    if response:
        response.headers["Cache-Control"] = "max-age=2"
    return data


@router.post("/", response_model=JobStatusResponse)
async def create_and_run_job(job_in: JobCreate, background_tasks: BackgroundTasks):
    manager = JobManager()

    job_id = await manager.create_job(job_in)
    await manager.start_discovery(job_id)
    background_tasks.add_task(_schedule_job, manager, job_id)

    job_data = await manager._table_select_single("job_batches", job_id)
    job_data["items"], _ = await manager.get_job_items(job_id, limit=20)
    return job_data


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    manager = JobManager()
    job_data = await manager._table_select_single("job_batches", job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    job_data["items"], _ = await manager.get_job_items(job_id, limit=20)
    return job_data


@router.get("/{job_id}/items", response_model=List[JobItemPreview])
async def get_job_items_only(job_id: str, response: Response, limit: int = 200):
    manager = JobManager()
    if limit > 1000:
        limit = 1000
    try:
        data, degraded = await manager.get_job_items(job_id, limit=limit)
    except Exception as e:
        response.headers["x-ops-degraded"] = "1"
        response.headers["Cache-Control"] = "max-age=2"
        return []

    if degraded:
        response.headers["x-ops-degraded"] = "1"
    response.headers["Cache-Control"] = "max-age=2"
    return data


@router.get("/{job_id}/summary")
async def get_job_summary(job_id: str, response: Response):
    manager = JobManager()
    try:
        summary, degraded = await manager.get_job_summary(job_id)
    except Exception as e:
        response.headers["x-ops-degraded"] = "1"
        response.headers["Cache-Control"] = "max-age=2"
        return {"job_id": job_id, "degraded": True, "error": str(e)}

    if summary is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if degraded:
        response.headers["x-ops-degraded"] = "1"
        summary["degraded"] = True
    else:
        summary["degraded"] = False
    response.headers["Cache-Control"] = "max-age=2"
    return summary
