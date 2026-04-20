# app/services/serp_service.py
import logging
import json
import asyncio
import httpx
import uuid
import time
import random
from typing import Optional, Dict, Any
from app.config import settings

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.dataforseo.com/v3/serp/google/organic"

_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=120.0)
    return _http_client


# In-memory job store.
# For persistence across Render restarts, replace with Redis or a DB table.
active_jobs: Dict[str, Dict[str, Any]] = {}


class SerpService:
    def __init__(self):
        self.username = settings.DFS_USERNAME or ""
        self.password = settings.DFS_PASSWORD or ""
        self.auth = (self.username, self.password)
        self.headers = {"Content-Type": "application/json"}
        logger.info("SerpService initialized")

    # ------------------------------------------------------------------
    # PUBLIC — called by routes
    # ------------------------------------------------------------------

    async def create_bulk_job(
        self,
        keywords: list[dict],  # [{"id": int, "keyword": str}, ...]
        depth: int = 100,
        on_complete=None,  # async callback(job: dict) when all results arrive
    ) -> str:
        """
        Submit all keywords to DataForSEO with a postback URL.
        Returns job_id immediately — results arrive via handle_postback().
        """
        job_id = str(uuid.uuid4())
        tag = str(random.randint(1, 10_000_000))

        active_jobs[job_id] = {
            "job_id": job_id,
            "tag": tag,
            "status": "processing",
            "created_at": time.time(),
            "last_postback_at": time.time(),
            "keywords": keywords,  # full list with ids
            "keyword_map": {kw["keyword"]: kw["id"] for kw in keywords},
            "depth": depth,
            "on_complete": on_complete,  # stored for later invocation
            "results": {},  # keyword -> list[item]
            "processed_count": 0,
        }

        asyncio.create_task(self._submit_all(job_id))
        asyncio.create_task(self._watchdog(job_id))

        logger.info(f"Bulk job {job_id} created — {len(keywords)} keywords, tag={tag}")
        return job_id

    def get_job_status(self, job_id: str) -> Optional[dict]:
        job = active_jobs.get(job_id)
        if not job:
            return None
        return {
            "job_id": job_id,
            "status": job["status"],
            "keywords_total": len(job["keywords"]),
            "keywords_done": job["processed_count"],
            "created_at": job["created_at"],
        }

    async def handle_postback(self, data: dict) -> bool:
        """
        Called by POST /serp/postback when DataForSEO delivers a result.
        Matches via tag, stores results, fires on_complete when all done.
        """
        try:
            tag = data["tasks"][0]["data"]["tag"]
        except (KeyError, IndexError):
            logger.error("Postback missing tag — ignoring")
            return False

        job = next((j for j in active_jobs.values() if j["tag"] == tag), None)
        if not job:
            logger.warning(f"No job found for postback tag={tag}")
            return False

        job["last_postback_at"] = time.time()

        parsed = self._parse_postback(data)
        for keyword, items in parsed.items():
            job["results"][keyword] = items
            job["processed_count"] += 1

        logger.info(
            f"Job {job['job_id']}: {job['processed_count']}/{len(job['keywords'])} done"
        )

        if job["processed_count"] >= len(job["keywords"]):
            await self._complete_job(job["job_id"])

        return True

    # ------------------------------------------------------------------
    # PRIVATE — submission
    # ------------------------------------------------------------------

    async def _submit_all(self, job_id: str):
        job = active_jobs.get(job_id)
        if not job:
            return

        keywords = [kw["keyword"] for kw in job["keywords"]]
        tag = job["tag"]
        depth = job["depth"]
        client = get_http_client()
        batch_size = 100

        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            payload = [
                {
                    "keyword": kw,
                    "location_code": 2208,
                    "language_code": "da",
                    "depth": depth,
                    "tag": tag,
                    "postback_url": f"{settings.API_BASE_URL}/api/serp/postback",
                    "postback_data": "advanced",
                }
                for kw in batch
            ]

            try:
                response = await client.post(
                    f"{API_BASE_URL}/task_post",
                    auth=self.auth,
                    headers=self.headers,
                    content=json.dumps(payload),
                    timeout=60.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    # Log the top-level status
                    logger.info(
                        f"Job {job_id}: API status {result.get('status_code')} "
                        f"— {result.get('status_message')}"
                    )
                    for task in result.get("tasks", []):
                        status = task.get("status_code")
                        kw = task.get("data", {}).get("keyword", "?")
                        msg = task.get("status_message", "")
                        if status == 20100:
                            logger.info(f"Job {job_id}: ✓ task created '{kw}'")
                        else:
                            logger.error(
                                f"Job {job_id}: ✗ task REJECTED '{kw}' "
                                f"— status={status} msg={msg}"
                            )
                else:
                    logger.error(
                        f"Job {job_id}: HTTP {response.status_code} — {response.text[:500]}"
                    )
            except Exception as e:
                logger.error(f"Job {job_id}: submit exception — {e}")

            if i + batch_size < len(keywords):
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # PRIVATE — watchdog
    # ------------------------------------------------------------------

    async def _watchdog(self, job_id: str, timeout_seconds: int = 600):
        """
        If no postback arrives for timeout_seconds, complete the job with
        whatever results have arrived so far — same pattern as jobQueue.py.
        """
        while True:
            await asyncio.sleep(30)

            job = active_jobs.get(job_id)
            if not job or job["status"] != "processing":
                return

            silence = time.time() - job["last_postback_at"]
            if silence >= timeout_seconds:
                logger.warning(
                    f"Job {job_id} watchdog timeout — "
                    f"{job['processed_count']}/{len(job['keywords'])} keywords received"
                )
                await self._complete_job(job_id)
                return

    # ------------------------------------------------------------------
    # PRIVATE — completion
    # ------------------------------------------------------------------

    async def _complete_job(self, job_id: str):
        job = active_jobs.get(job_id)
        if not job or job["status"] == "complete":
            return

        job["status"] = "complete"
        logger.info(
            f"Job {job_id} complete — {len(job['results'])}/{len(job['keywords'])} "
            f"keywords returned results"
        )

        # Invoke the caller-supplied callback (e.g. run_bulk_refresh webhook logic)
        if job.get("on_complete"):
            try:
                await job["on_complete"](job)
            except Exception as e:
                logger.error(f"Job {job_id} on_complete callback failed: {e}")

        # Clean up after a short delay so callers can still read status
        await asyncio.sleep(300)
        active_jobs.pop(job_id, None)

    # ------------------------------------------------------------------
    # PRIVATE — parsing
    # ------------------------------------------------------------------

    def _parse_postback(self, data: dict) -> dict[str, list]:
        results: dict[str, list] = {}
        try:
            for task in data.get("tasks", []):
                for result in task.get("result", []):
                    keyword = result.get("keyword", "unknown")
                    items = [
                        {
                            "type": item.get("type"),
                            "rank_group": item.get("rank_group"),
                            "rank_absolute": item.get("rank_absolute"),
                            "item_position": item.get("position", 0),
                            "domain": item.get("domain"),
                            "url": item.get("url"),
                            "title": item.get("title"),
                            "description": item.get("description"),
                        }
                        for item in result.get("items", [])
                    ]
                    results[keyword] = items
        except Exception as e:
            logger.error(f"Error parsing postback: {e}")
        return results
