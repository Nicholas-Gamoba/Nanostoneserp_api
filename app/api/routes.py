# app/api/routes.py
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from app.services.webhook_service import WebhookService
from app.services.serp_service import SerpService
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()
webhook_service = WebhookService()
serp_service = SerpService()


class SerpRequest(BaseModel):
    keyword_id: int
    keyword: str
    country: str
    language: str


async def run_serp_and_notify(
    job_id: str,
    body: SerpRequest,
    callback_url: str | None,
    start_time: datetime,
):
    """Background task: fetch SERP results then fire webhook"""
    try:
        results = await serp_service.fetch_serp_results(
            keyword=body.keyword,
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"SERP search complete — {len(results)} items in {duration:.2f}s")

        result = {
            "status": "success",
            "job_id": job_id,
            "keyword_id": body.keyword_id,
            "keyword": body.keyword,
            "country": body.country,
            "language": body.language,
            "serp_date": end_time.isoformat(),
            "items": results,
            "item_count": len(results),
            "started_at": start_time.isoformat(),
            "completed_at": end_time.isoformat(),
            "duration_seconds": duration,
        }

        await webhook_service.send_webhook(
            result_data=result,
            origin_url=callback_url,
            webhook_path="/api/webhook/serp-completed",
        )

    except Exception as e:
        logger.error(f"Background SERP task failed for job_id {job_id}: {e}")


async def run_bulk_refresh(
    keywords: list[dict],
    callback_url: str | None,
    start_time: datetime,
):
    """Background task: fetch all keywords in one batch then fire webhooks"""
    keyword_strings = [kw["keyword"] for kw in keywords]
    keyword_id_map = {kw["keyword"]: kw["id"] for kw in keywords}

    logger.info(f"Starting bulk refresh for {len(keywords)} keywords...")

    try:
        all_results = await serp_service.fetch_serp_results_bulk(keyword_strings)
    except Exception as e:
        logger.error(f"Bulk SERP refresh failed: {e}")
        return

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Bulk refresh complete in {duration:.2f}s")

    # Fire one webhook per keyword with its results
    for keyword, items in all_results.items():
        keyword_id = keyword_id_map.get(keyword, 0)
        job_id = f"serp_{keyword_id}_{int(start_time.timestamp())}"

        result = {
            "status": "success",
            "job_id": job_id,
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

        try:
            await webhook_service.send_webhook(
                result_data=result,
                origin_url=callback_url,
                webhook_path="/api/webhook/serp-completed",
            )
            logger.info(f"Webhook sent for '{keyword}' ({len(items)} items)")
        except Exception as e:
            logger.error(f"Webhook failed for '{keyword}': {e}")


@router.post("/serp/search")
async def run_serp_search(
    body: SerpRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Kick off a single SERP search in the background, return job_id immediately"""
    start_time = datetime.now()
    job_id = f"serp_{body.keyword_id}_{int(start_time.timestamp())}"

    logger.info(f"SERP job queued — job_id: {job_id}, keyword: '{body.keyword}'")

    background_tasks.add_task(
        run_serp_and_notify,
        job_id=job_id,
        body=body,
        callback_url=request.headers.get("X-Callback-URL"),
        start_time=start_time,
    )

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
    """Fetch all keywords from DB and refresh them all in a single batch"""
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
    logger.info(f"Queueing {len(keywords)} keywords for bulk refresh...")

    background_tasks.add_task(
        run_bulk_refresh,
        keywords=keywords,
        callback_url=settings.VERCEL_WEBHOOK_URL,
        start_time=start_time,
    )

    return {
        "status": "queued",
        "keyword_count": len(keywords),
        "message": f"Bulk refresh started for {len(keywords)} keywords",
    }


@router.get("/serp/health")
def serp_health():
    return {"status": "healthy", "service": "serp_api"}
