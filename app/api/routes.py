# app/api/routes.py
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

# Limit concurrent outbound webhooks to avoid exhausting Vercel's Prisma connection pool
_webhook_semaphore = asyncio.Semaphore(10)


class SerpRequest(BaseModel):
    keyword_id: int
    keyword: str
    country: str
    language: str


# ------------------------------------------------------------------
# Webhook helpers
# ------------------------------------------------------------------


def _make_keyword_complete_handler(callback_url: str | None, start_time: datetime):
    """
    Returns an async callback that fires a webhook when a single keyword's
    postback arrives. Webhook send is detached via asyncio.create_task so
    it does NOT block the postback response to DataForSEO.
    Throttled to 10 concurrent webhooks via semaphore.
    """

    async def on_keyword_complete(
        keyword: str, keyword_id: int, items: list, job: dict
    ):
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            "status": "success",
            "job_id": job["job_id"],
            "keyword_id": keyword_id,
            "keyword": keyword,
            "country": "Denmark",
            "language": "Danish",
            "serp_date": end_time.isoformat(),
            "items": items,
            "item_count": len(items),
            "started_at": start_time.isoformat(),
            "completed_at": end_time.isoformat(),
            "duration_seconds": duration,
        }

        async def _send():
            async with _webhook_semaphore:
                try:
                    await webhook_service.send_webhook(
                        result_data=result,
                        origin_url=callback_url,
                        webhook_path="/api/webhook/serp-completed",
                    )
                    logger.info(f"Webhook sent for '{keyword}' ({len(items)} items)")
                except Exception as e:
                    logger.error(f"Webhook failed for '{keyword}': {e}")

        # Fire and forget — don't block the postback response on Vercel's latency.
        asyncio.create_task(_send())

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
        import asyncpg

        conn = await asyncpg.connect(settings.DATABASE_URL)
        rows = await conn.fetch('SELECT id, keyword FROM "SEOKeyword" ORDER BY id')
        await conn.close()
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