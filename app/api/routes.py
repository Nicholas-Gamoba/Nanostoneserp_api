# app/api/routes.py
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from app.services.webhook_service import WebhookService
from app.services.serp_service import SerpService

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
        await webhook_service.send_webhook(
            result_data={"status": "failed", "job_id": job_id, "error": str(e)},
            origin_url=callback_url,
            webhook_path="/api/webhook/serp-completed",
        )


@router.post("/serp/search")
async def run_serp_search(
    body: SerpRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Kick off a SERP search in the background, return job_id immediately"""
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


@router.get("/serp/health")
def serp_health():
    return {"status": "healthy", "service": "serp_api"}
