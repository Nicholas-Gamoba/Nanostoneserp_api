# app/services/serp_service.py
import logging
import json
import asyncio
import httpx
import uuid
import random
import datetime
from typing import Optional, Dict, Any, Callable
from app.config import settings
from app.services.regression_service import get_due_rechecks

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.dataforseo.com/v3/serp/google/organic"

_http_client: Optional[httpx.AsyncClient] = None

# --- Connection pool (replaces per-call asyncpg.connect) ---
_db_pool = None


async def get_db_pool():
    global _db_pool
    if _db_pool is None:
        import asyncpg
        _db_pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            min_size=2,
            max_size=10,  # tune to your Postgres plan
        )
    return _db_pool


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=120.0)
    return _http_client


_job_callbacks: Dict[str, Dict[str, Any]] = {}

# --- Single shared watchdog instead of one per job ---
_watchdog_task: Optional[asyncio.Task] = None


def _ensure_watchdog_running():
    """Start the global watchdog task if it isn't already running."""
    global _watchdog_task
    if _watchdog_task is None or _watchdog_task.done():
        _watchdog_task = asyncio.create_task(_global_watchdog())


class SerpService:
    def __init__(self):
        self.username = settings.DFS_USERNAME or ""
        self.password = settings.DFS_PASSWORD or ""
        self.auth = (self.username, self.password)
        self.headers = {"Content-Type": "application/json"}
        logger.info("SerpService initialized")

    # ------------------------------------------------------------------
    # PUBLIC
    # ------------------------------------------------------------------

    async def create_bulk_job(
        self,
        keywords: list[dict],
        depth: int = 20,
        on_keyword_complete: Optional[Callable] = None,
        on_complete: Optional[Callable] = None,
    ) -> str:
        depth = min(depth, 20)
        job_id = str(uuid.uuid4())
        tag = str(random.randint(1, 10_000_000))

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO serp_jobs
                    (job_id, tag, status, depth, keywords_total, processed_count,
                     last_postback_at)
                VALUES ($1, $2, 'processing', $3, $4, 0, NOW())
                """,
                job_id, tag, depth, len(keywords),
            )
            await conn.executemany(
                """
                INSERT INTO serp_job_keywords (job_id, keyword_id, keyword, done)
                VALUES ($1, $2, $3, false)
                """,
                [(job_id, kw["id"], kw["keyword"]) for kw in keywords],
            )

        _job_callbacks[job_id] = {
            "on_keyword_complete": on_keyword_complete,
            "on_complete": on_complete,
        }

        asyncio.create_task(self._submit_all(job_id, keywords, tag, depth))
        _ensure_watchdog_running()  # one shared task, not one per job

        logger.info(f"Bulk job {job_id} created — {len(keywords)} keywords, tag={tag}")
        return job_id

    async def get_job_status(self, job_id: str) -> Optional[dict]:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM serp_jobs WHERE job_id = $1", job_id
            )
        if not row:
            return None
        return {
            "job_id": job_id,
            "status": row["status"],
            "keywords_total": row["keywords_total"],
            "keywords_done": row["processed_count"],
            "created_at": row["created_at"].timestamp(),
        }

    async def handle_postback(self, data: dict) -> bool:
        try:
            tag = data["tasks"][0]["data"]["tag"]
        except (KeyError, IndexError):
            logger.error("Postback missing tag — ignoring")
            return False

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            job_row = await conn.fetchrow(
                "SELECT * FROM serp_jobs WHERE tag = $1", tag
            )
            if not job_row:
                logger.warning(f"No job found for postback tag={tag}")
                return False
            if job_row["status"] == "complete":
                return True

            job_id = job_row["job_id"]
            parsed = self._parse_postback(data)

            for keyword, items in parsed.items():
                kw_row = await conn.fetchrow(
                    """
                    SELECT keyword_id FROM serp_job_keywords
                    WHERE job_id = $1 AND keyword = $2
                    """,
                    job_id, keyword,
                )
                if not kw_row:
                    logger.warning(
                        f"Job {job_id}: no keyword_id for '{keyword}' — skipping"
                    )
                    continue

                keyword_id = kw_row["keyword_id"]

                await conn.execute(
                    "UPDATE serp_job_keywords SET done = true WHERE job_id = $1 AND keyword = $2",
                    job_id, keyword,
                )
                await conn.execute(
                    """
                    UPDATE serp_jobs
                    SET processed_count = processed_count + 1,
                        last_postback_at = NOW()
                    WHERE job_id = $1
                    """,
                    job_id,
                )

                callbacks = _job_callbacks.get(job_id, {})
                cb = callbacks.get("on_keyword_complete")
                if cb:
                    try:
                        await cb(keyword, keyword_id, items, {"job_id": job_id, "tag": tag})
                    except Exception as e:
                        logger.error(
                            f"Job {job_id}: on_keyword_complete failed for '{keyword}': {e}"
                        )

            updated = await conn.fetchrow(
                "SELECT processed_count, keywords_total FROM serp_jobs WHERE job_id = $1",
                job_id,
            )

        logger.info(
            f"Job {job_id}: {updated['processed_count']}/{updated['keywords_total']} done"
        )

        if updated["processed_count"] >= updated["keywords_total"]:
            await self._complete_job(job_id)

        return True

    # ------------------------------------------------------------------
    # PRIVATE — submission
    # ------------------------------------------------------------------

    async def _submit_all(self, job_id: str, keywords: list[dict], tag: str, depth: int):
        client = get_http_client()
        batch_size = 100
        kw_strings = [kw["keyword"] for kw in keywords]

        for i in range(0, len(kw_strings), batch_size):
            batch = kw_strings[i: i + batch_size]
            payload = [
                {
                    "keyword": kw,
                    "location_code": 2208,
                    "language_code": "da",
                    "depth": depth,
                    "tag": tag,
                    "postback_url": f"{settings.API_BASE_URL}/api/serp/postback",
                    "postback_data": "regular",
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
                    logger.info(
                        f"Job {job_id}: API status {result.get('status_code')} "
                        f"— {result.get('status_message')}"
                    )
                    for task in result.get("tasks", []):
                        status = task.get("status_code")
                        kw = task.get("data", {}).get("keyword", "?")
                        msg = task.get("status_message", "")
                        if status == 20100:
                            logger.debug(f"Job {job_id}: ✓ task created '{kw}'")
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

            if i + batch_size < len(kw_strings):
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # PRIVATE — single shared watchdog (replaces per-job watchdog)
    # ------------------------------------------------------------------

    async def _complete_job(self, job_id: str):
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, processed_count, keywords_total FROM serp_jobs WHERE job_id = $1",
                job_id,
            )
            if not row or row["status"] == "complete":
                return
            await conn.execute(
                "UPDATE serp_jobs SET status = 'complete' WHERE job_id = $1", job_id
            )

        logger.info(
            f"Job {job_id} complete — "
            f"{row['processed_count']}/{row['keywords_total']} keywords processed"
        )

        callbacks = _job_callbacks.pop(job_id, None)
        if callbacks and callbacks.get("on_complete"):
            try:
                await callbacks["on_complete"]({"job_id": job_id})
            except Exception as e:
                logger.error(f"Job {job_id} on_complete callback failed: {e}")

    # ------------------------------------------------------------------
    # PRIVATE — parsing
    # ------------------------------------------------------------------

    def _parse_postback(self, data: dict) -> dict[str, list]:
        results: dict[str, list] = {}
        try:
            for task in data.get("tasks", []):
                for result in task.get("result", []):
                    keyword = result.get("keyword", "unknown")
                    results[keyword] = [
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
        except Exception as e:
            logger.error(f"Error parsing postback: {e}")
        return results

    async def schedule_recheck(self, delay_seconds: int = 10800, on_keyword_complete=None):
        logger.info(f"Recheck scheduled in {delay_seconds}s")
        await asyncio.sleep(delay_seconds)

        due = await get_due_rechecks()
        if not due:
            logger.info("Recheck fired but nothing in queue — skipping")
            return

        keywords = [{"id": r["keyword_id"], "keyword": r["keyword"]} for r in due]
        logger.info(f"Recheck launching — {len(keywords)} keywords")

        await self.create_bulk_job(
            keywords=keywords,
            on_keyword_complete=on_keyword_complete,
        )


# ------------------------------------------------------------------
# Module-level shared watchdog — one loop for ALL active jobs
# ------------------------------------------------------------------

async def _global_watchdog(poll_interval: int = 30, timeout_seconds: int = 600):
    """
    Single coroutine that scans all processing jobs every `poll_interval`
    seconds and times out any that have gone silent.
    Replaces the per-job _watchdog to avoid O(n_jobs) coroutine overhead.
    """
    logger.info("Global SERP watchdog started")
    while True:
        await asyncio.sleep(poll_interval)
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT job_id, last_postback_at, processed_count, keywords_total
                    FROM serp_jobs
                    WHERE status = 'processing'
                    """
                )

            if not rows:
                # No active jobs — exit; will be restarted on next create_bulk_job
                logger.info("Global watchdog: no active jobs, exiting")
                return

            now = datetime.datetime.now(datetime.timezone.utc)
            for row in rows:
                last = row["last_postback_at"]
                if last is None:
                    continue  # job just created, give it time
                silence = (now - last).total_seconds()
                if silence >= timeout_seconds:
                    logger.warning(
                        f"Watchdog timeout for job {row['job_id']} — "
                        f"{row['processed_count']}/{row['keywords_total']} received, "
                        f"{silence:.0f}s since last postback"
                    )
                    # Import here to avoid circular; SerpService._complete_job
                    # needs an instance but the logic is stateless — call directly.
                    await _complete_job_standalone(row["job_id"])

        except Exception as e:
            logger.error(f"Global watchdog error: {e}")


async def _complete_job_standalone(job_id: str):
    """Stateless version of _complete_job usable outside SerpService instance."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, processed_count, keywords_total FROM serp_jobs WHERE job_id = $1",
            job_id,
        )
        if not row or row["status"] == "complete":
            return
        await conn.execute(
            "UPDATE serp_jobs SET status = 'complete' WHERE job_id = $1", job_id
        )

    logger.info(
        f"Job {job_id} force-completed by watchdog — "
        f"{row['processed_count']}/{row['keywords_total']} keywords processed"
    )

    callbacks = _job_callbacks.pop(job_id, None)
    if callbacks and callbacks.get("on_complete"):
        try:
            await callbacks["on_complete"]({"job_id": job_id})
        except Exception as e:
            logger.error(f"Job {job_id} on_complete callback failed: {e}")