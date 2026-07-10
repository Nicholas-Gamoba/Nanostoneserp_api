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

# Neon scales its compute to zero after a few minutes idle. Waking it
# ("cold start") can take 15-30s+, during which connections time out or are
# refused. These budgets must comfortably exceed that wake time — the old
# 10s acquire timeout was shorter than the cold start, so the 00:00 cron
# (first DB hit after an idle night) 500'd on every run and the every-3-day
# full refresh silently stopped producing data.
DB_CONNECT_TIMEOUT = 60.0   # per-connection establishment budget
DB_POOL_INIT_RETRIES = 5


async def get_db_pool():
    global _db_pool
    if _db_pool is None:
        import asyncpg

        last_err = None
        for attempt in range(1, DB_POOL_INIT_RETRIES + 1):
            try:
                _db_pool = await asyncpg.create_pool(
                    settings.DATABASE_URL,
                    min_size=2,  # fewer upfront connections -> faster cold-start init
                    max_size=25,
                    command_timeout=30.0,
                    timeout=DB_CONNECT_TIMEOUT,  # connection-establishment timeout
                )
                return _db_pool
            except Exception as e:
                last_err = e
                logger.warning(
                    f"DB pool init attempt {attempt}/{DB_POOL_INIT_RETRIES} failed: "
                    f"{type(e).__name__}: {e!r} — retrying"
                )
                await asyncio.sleep(min(3 * attempt, 15))
        raise RuntimeError(f"Could not initialize DB pool: {last_err!r}")
    return _db_pool


async def ensure_db_ready(max_attempts: int = 5) -> None:
    """
    Guarantee the database is actually reachable *right now*, waking a
    suspended Neon compute and rebuilding a stale pool if needed.

    Call this at the start of cron-triggered entry points (the first DB hit
    after a long idle period) before running the real query. Both failure
    modes are covered:
      * cold Neon  -> connection establishment is retried with backoff
      * stale pool -> a failing `SELECT 1` drops the dead pool so it rebuilds
    """
    global _db_pool
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            if attempt > 1:
                logger.info(f"DB ready after {attempt} attempts")
            return
        except Exception as e:
            last_err = e
            logger.warning(
                f"DB not ready (attempt {attempt}/{max_attempts}): "
                f"{type(e).__name__}: {e!r}"
            )
            # Drop a possibly-dead pool so the next attempt rebuilds it.
            if _db_pool is not None:
                try:
                    await _db_pool.close()
                except Exception:
                    pass
                _db_pool = None
            await asyncio.sleep(min(3 * attempt, 15))
    raise RuntimeError(f"DB not reachable after {max_attempts} attempts: {last_err!r}")


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
                job_id,
                tag,
                depth,
                len(keywords),
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

        parsed = self._parse_postback(data)
        pool = await get_db_pool()

        job_id = None
        processed_count = 0
        keywords_total = 0

        async with pool.acquire() as conn:
            job_row = await conn.fetchrow("SELECT * FROM serp_jobs WHERE tag = $1", tag)
            if not job_row:
                logger.warning(f"No job found for postback tag={tag}")
                return False
            if job_row["status"] == "complete":
                return True

            job_id = job_row["job_id"]
            callbacks = _job_callbacks.get(job_id, {})
            cb = callbacks.get("on_keyword_complete")

            for keyword, items in parsed.items():
                async with conn.transaction():
                    updated = await conn.fetchrow(
                        """
                        UPDATE serp_job_keywords
                        SET done = true
                        WHERE job_id = $1 AND keyword = $2 AND done = false
                        RETURNING keyword_id
                        """,
                        job_id,
                        keyword,
                    )
                    if not updated:
                        logger.debug(
                            f"Job {job_id}: duplicate or unknown keyword '{keyword}' — skipping"
                        )
                        continue

                    keyword_id = updated["keyword_id"]
                    serp_date = datetime.datetime.now(datetime.timezone.utc)

                    await conn.execute(
                        """
                        UPDATE serp_jobs
                        SET processed_count = processed_count + 1,
                            last_postback_at = NOW()
                        WHERE job_id = $1
                        """,
                        job_id,
                    )

                    rows_to_insert = [
                        (
                            keyword_id,
                            item["type"],
                            item["rank_group"],
                            item["rank_absolute"],
                            item["item_position"],
                            item["domain"],
                            item["url"],
                            item["title"],
                            item["description"],
                            serp_date,
                        )
                        for item in items
                        if item.get("type") and item.get("rank_absolute") is not None
                    ]
                    if rows_to_insert:
                        await conn.executemany(
                            """
                            INSERT INTO serp_results
                                (keyword_id, country, language, type, rank_group, rank_absolute,
                                item_position, domain, url, title, description, serp_date)
                            VALUES ($1, 'Denmark', 'Danish', $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT (keyword_id, country, language, type, rank_absolute, serp_date)
                            DO UPDATE SET
                                rank_group    = EXCLUDED.rank_group,
                                item_position = EXCLUDED.item_position,
                                domain        = EXCLUDED.domain,
                                url           = EXCLUDED.url,
                                title         = EXCLUDED.title,
                                description   = EXCLUDED.description,
                                updated_at    = NOW()
                            """,
                            rows_to_insert,
                        )
                        logger.debug(
                            f"Job {job_id}: saved {len(rows_to_insert)} rows for '{keyword}'"
                        )

                if cb:
                    try:
                        await cb(
                            keyword,
                            keyword_id,
                            items,
                            {"job_id": job_id, "tag": tag, "serp_date": serp_date.isoformat()},
                        )
                    except Exception as e:
                        logger.error(
                            f"Job {job_id}: on_keyword_complete failed for '{keyword}': {e}"
                        )

                items = None

            updated = await conn.fetchrow(
                "SELECT processed_count, keywords_total FROM serp_jobs WHERE job_id = $1",
                job_id,
            )
            processed_count = updated["processed_count"]
            keywords_total = updated["keywords_total"]

        logger.info(f"Job {job_id}: {processed_count}/{keywords_total} done")

        if processed_count >= keywords_total:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                sub_done = await conn.fetchval(
                    "SELECT submission_complete FROM serp_jobs WHERE job_id = $1",
                    job_id,
                )
            if sub_done:
                await self._complete_job(job_id)
            else:
                logger.info(
                    f"Job {job_id}: count met but submission still in progress — "
                    f"deferring completion"
                )

        return True

    # ------------------------------------------------------------------
    # PRIVATE — submission
    # ------------------------------------------------------------------

    async def _submit_all(self, job_id, keywords, tag, depth):
        client = get_http_client()
        batch_size = 100

        for i in range(0, len(keywords), batch_size):
            batch = keywords[i : i + batch_size]  # slice dicts, not a pre-built list
            payload = [
                {
                    "keyword": kw["keyword"],
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
                    rejected_keywords = []
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
                            rejected_keywords.append(kw)

                    if rejected_keywords:
                        pool = await get_db_pool()
                        async with pool.acquire() as conn:
                            # Remove rejected keywords from the job so keywords_total reflects
                            # what will actually come back via postback.
                            await conn.execute(
                                """
                                DELETE FROM serp_job_keywords
                                WHERE job_id = $1 AND keyword = ANY($2::text[])
                                """,
                                job_id,
                                rejected_keywords,
                            )
                            await conn.execute(
                                """
                                UPDATE serp_jobs
                                SET keywords_total = keywords_total - $2
                                WHERE job_id = $1
                                """,
                                job_id,
                                len(rejected_keywords),
                            )
                        logger.warning(
                            f"Job {job_id}: removed {len(rejected_keywords)} rejected keywords "
                            f"from total"
                        )
                else:
                    logger.error(
                        f"Job {job_id}: HTTP {response.status_code} — {response.text[:500]}"
                    )
            except Exception as e:
                logger.error(f"Job {job_id}: submit exception — {e}")

            if i + batch_size < len(keywords):
                await asyncio.sleep(1)

        # All batches submitted — now postbacks can complete the job.
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE serp_jobs SET submission_complete = true WHERE job_id = $1",
                job_id,
            )
        logger.info(f"Job {job_id}: submission complete, {len(keywords)} keywords sent")

        # If postbacks already drained while we were still submitting,
        # the last postback's completion check would have been skipped.
        # Re-check now.
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT processed_count, keywords_total FROM serp_jobs "
                "WHERE job_id = $1 AND status = 'processing'",
                job_id,
            )
        if row and row["processed_count"] >= row["keywords_total"]:
            await self._complete_job(job_id)

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

    async def schedule_recheck(
        self, delay_seconds: int = 10800, on_keyword_complete=None
    ):
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


async def _global_watchdog(poll_interval: int = 30, timeout_seconds: int = 1800):
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
                    SELECT job_id, last_postback_at, processed_count, keywords_total,
                           submission_complete
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
                if not row["submission_complete"]:
                    continue  # don't time out a job mid-submission
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
