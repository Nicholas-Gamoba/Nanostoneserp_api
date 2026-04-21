import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.api.routes import router as api_router, serp_service, _make_keyword_complete_handler
from app.services.regression_service import get_due_rechecks, get_pending_rechecks

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    due = await get_due_rechecks()
    if due:
        logger.info(f"Startup: {len(due)} overdue rechecks — submitting immediately")
        keywords = [{"id": r["keyword_id"], "keyword": r["keyword"]} for r in due]
        handler = _make_keyword_complete_handler(settings.VERCEL_WEBHOOK_URL, datetime.now())
        await serp_service.create_bulk_job(keywords=keywords, on_keyword_complete=handler)

    pending = await get_pending_rechecks()
    for row in pending:
        delay = max(0, (row["run_after"] - datetime.now(timezone.utc)).total_seconds())
        logger.info(f"Startup: rescheduling '{row['keyword']}' in {delay:.0f}s")
        handler = _make_keyword_complete_handler(settings.VERCEL_WEBHOOK_URL, datetime.now())
        asyncio.create_task(
            serp_service.schedule_recheck(delay_seconds=int(delay), on_keyword_complete=handler)
        )

    yield


app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")


@app.get("/")
def read_root():
    return {
        "message": "SERP API",
        "version": settings.API_VERSION,
        "status": "running",
        "endpoints": [
            "POST /api/serp/search          — trigger a DataForSEO SERP search",
            "POST /api/serp/refresh-all     — bulk refresh all keywords",
            "POST /api/serp/check-regressions — compare latest two runs",
            "POST /api/serp/postback        — receive DataForSEO results callback",
            "GET  /api/serp/jobs/{job_id}   — poll job status",
        ],
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8002, reload=True)