import logging
import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from app.services.webhook_service import WebhookService
from app.services.serp_service import SerpService
from app.config import settings
from app.services.regression_service import queue_regressions
import gzip
import json

logger = logging.getLogger(__name__)

router = APIRouter()
webhook_service = WebhookService()
serp_service = SerpService()

# Bounded queue prevents unbounded memory growth.
# If queue fills, postback handler will await — backpressure on DataForSEO.
_webhook_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
_webhook_workers_started = False
NUM_WEBHOOK_WORKERS = 10


async def _webhook_worker():
    """Worker pulls from the queue and sends webhooks one at a time."""
    while True:
        result, callback_url, keyword, item_count = await _webhook_queue.get()
        try:
            await webhook_service.send_webhook(
                result_data=result,
                origin_url=callback_url,
                webhook_path="/api/webhook/serp-completed",
            )
            logger.info(f"Webhook sent for '{keyword}' ({item_count} items)")
        except Exception as e:
            logger.error(f"Webhook failed for '{keyword}': {e}")
        finally:
            _webhook_queue.task_done()


def _ensure_workers_started():
    global _webhook_workers_started
    if not _webhook_workers_started:
        for _ in range(NUM_WEBHOOK_WORKERS):
            asyncio.create_task(_webhook_worker())
        _webhook_workers_started = True
        logger.info(f"Started {NUM_WEBHOOK_WORKERS} webhook workers")


class SerpRequest(BaseModel):
    keyword_id: int
    keyword: str
    country: str
    language: str


def _make_keyword_complete_handler(callback_url: str | None, start_time: datetime):
    """
    Returns a callback that enqueues a best-effort webhook notification.
    Results are already written to serp_results before this fires — a dropped
    webhook no longer means lost data.
    """

    async def on_keyword_complete(
        keyword: str, keyword_id: int, items: list, job: dict
    ):
        _ensure_workers_started()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            "status": "success",
            "job_id": job["job_id"],
            "keyword_id": keyword_id,
            "keyword": keyword,
            "country": "Denmark",
            "language": "Danish",
            "serp_date": job["serp_date"],
            "items": items,
            "item_count": len(items),
            "started_at": start_time.isoformat(),
            "completed_at": end_time.isoformat(),
            "duration_seconds": duration,
        }

        try:
            _webhook_queue.put_nowait((result, callback_url, keyword, len(items)))
        except asyncio.QueueFull:
            logger.warning(
                f"Webhook queue full — dropping notification for '{keyword}'. "
                f"Results already saved to DB."
            )

    return on_keyword_complete


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------


@router.post("/serp/check-regressions")
async def check_regressions(request: Request):
    cron_secret = request.headers.get("X-Cron-Secret")
    if cron_secret != settings.CRON_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    result = await queue_regressions()

    lost_count = len(result.get("lost", []))
    moved_count = len(result.get("moved_5", []))

    if lost_count > 0 or moved_count > 0:
        handler = _make_keyword_complete_handler(
            settings.VERCEL_WEBHOOK_URL, datetime.now()
        )
        asyncio.create_task(
            serp_service.schedule_recheck(
                delay_seconds=10800, on_keyword_complete=handler
            )
        )
        logger.info("Recheck task created — will fire in 3h")

    return result


@router.post("/serp/search")
async def run_serp_search(
    body: SerpRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Kick off a single-keyword SERP search.
    Internally uses the same bulk/postback path — returns job_id immediately.
    """
    start_time = datetime.now()
    callback_url = request.headers.get("X-Callback-URL")

    job_id = await serp_service.create_bulk_job(
        keywords=[{"id": body.keyword_id, "keyword": body.keyword}],
        on_keyword_complete=_make_keyword_complete_handler(callback_url, start_time),
    )

    logger.info(f"Single SERP job queued — job_id: {job_id}, keyword: '{body.keyword}'")

    return {
        "status": "queued",
        "job_id": job_id,
        "keyword_id": body.keyword_id,
        "keyword": body.keyword,
        "message": "SERP search started, results will be sent via webhook",
    }


@router.post("/serp/refresh-all")
async def refresh_all_keywords(
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Fetch all keywords from DB and refresh them all via postback."""
    cron_secret = request.headers.get("X-Cron-Secret")
    if cron_secret != settings.CRON_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        from app.services.serp_service import get_db_pool
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT id, keyword FROM "SEOKeyword" ORDER BY id')
        keywords = [{"id": row["id"], "keyword": row["keyword"]} for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch keywords from DB: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch keywords: {e}")
    if not keywords:
        return {"status": "no_keywords", "message": "No keywords found"}

    start_time = datetime.now()
    callback_url = settings.VERCEL_WEBHOOK_URL

    job_id = await serp_service.create_bulk_job(
        keywords=keywords,
        on_keyword_complete=_make_keyword_complete_handler(callback_url, start_time),
    )

    logger.info(f"Bulk refresh queued — job_id: {job_id}, {len(keywords)} keywords")

    return {
        "status": "queued",
        "job_id": job_id,
        "keyword_count": len(keywords),
        "message": f"Bulk refresh started for {len(keywords)} keywords",
    }


@router.post("/serp/postback")
async def serp_postback(request: Request):
    body = await request.body()
    try:
        decompressed = gzip.decompress(body)
        data = json.loads(decompressed)
    except gzip.BadGzipFile:
        data = json.loads(body)

    await serp_service.handle_postback(data)
    return {"status": "ok"}


@router.get("/serp/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll job progress — useful for debugging or UI status indicators."""
    status = await serp_service.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@router.get("/serp/health")
def serp_health():
    return {"status": "healthy", "service": "serp_api"}