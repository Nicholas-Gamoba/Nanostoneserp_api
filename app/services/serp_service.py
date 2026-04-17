# app/services/serp_service.py
import logging
import json
import asyncio
import httpx
from collections import deque
from datetime import datetime, timedelta
from typing import Optional
from app.config import settings

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.dataforseo.com/v3/serp/google/organic"

# Shared client — one connection pool for the lifetime of the process
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=120.0)
    return _http_client


class RateLimiter:
    """Sliding window rate limiter — max N requests per 60 seconds"""

    def __init__(self, max_per_minute: int = 18):
        self.max_per_minute = max_per_minute
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = datetime.utcnow()
            cutoff = now - timedelta(seconds=60)
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_per_minute:
                wait_until = self._timestamps[0] + timedelta(seconds=60)
                wait_seconds = (wait_until - now).total_seconds() + 0.5
                logger.info(f"Rate limit reached — waiting {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                now = datetime.utcnow()
                cutoff = now - timedelta(seconds=60)
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()

            self._timestamps.append(datetime.utcnow())


# Module-level singletons
_rate_limiter = RateLimiter(max_per_minute=18)

# Controls how many chunks can be anywhere in the pipeline at once
# (submitted, polling, or fetching). This is the memory knob.
# 3 chunks × ~15MB each = ~45MB peak — well within 512MB.
# Increase to 4 or 5 for more speed; each step adds ~15MB peak.
_pipeline_semaphore = asyncio.Semaphore(3)

# How many keywords to process per chunk.
# At 100 results/keyword, 15 keywords ≈ ~15MB peak per chunk — safe for 512MB Render tier.
CHUNK_SIZE = 15


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

    async def fetch_serp_results(
        self,
        keyword: str,
        depth: int = 100,
    ) -> list:
        """Single keyword: convenience wrapper around bulk."""
        results = await self.fetch_serp_results_bulk([keyword], depth=depth)
        return results.get(keyword, [])

    async def fetch_serp_results_bulk(
        self,
        keywords: list[str],
        depth: int = 100,
    ) -> dict[str, list]:
        """
        Process any number of keywords safely on a 512MB instance.

        Strategy:
          - Split keywords into chunks of CHUNK_SIZE
          - All chunks are fired concurrently, but _pipeline_semaphore limits
            how many are active (submitted + polling + fetching) at once
          - While one chunk is polling (sleeping), others can be polling too —
            DataForSEO wait time is pure I/O so parallelising it is free
          - Results are stored outside the semaphore, so memory is bounded
            to (semaphore size × chunk memory) at any point

        For 2500 keywords at CHUNK_SIZE=15 and semaphore=3: ~167 chunks,
        ~3 running concurrently → roughly 3x faster than pure sequential.
        Total wall time: ~2 hours vs ~6 hours sequential.
        Callers should run this as a background job.
        """
        all_items: dict[str, list] = {kw: [] for kw in keywords}

        chunks = [
            keywords[i : i + CHUNK_SIZE] for i in range(0, len(keywords), CHUNK_SIZE)
        ]
        total_chunks = len(chunks)
        logger.info(
            f"Processing {len(keywords)} keywords in {total_chunks} chunks of {CHUNK_SIZE}"
        )

        async def process_chunk(i: int, chunk: list[str]):
            # Semaphore held through the entire submit→poll→fetch→parse lifecycle.
            # Released before storing results, so peak memory stays bounded.
            async with _pipeline_semaphore:
                logger.info(
                    f"--- Chunk {i + 1}/{total_chunks} ({len(chunk)} keywords) ---"
                )
                chunk_results = await self._fetch_chunk(chunk, depth)
            # Store outside semaphore — just dict updates, negligible memory
            all_items.update(chunk_results)

        await asyncio.gather(
            *[process_chunk(i, chunk) for i, chunk in enumerate(chunks)]
        )

        logger.info(f"All {len(keywords)} keywords complete.")
        return all_items

    # ------------------------------------------------------------------
    # PRIVATE — chunk orchestration
    # ------------------------------------------------------------------

    async def _fetch_chunk(self, keywords: list[str], depth: int) -> dict[str, list]:
        """Submit one chunk, poll until ready, fetch results, return parsed items."""
        payload = [
            {
                "keyword": kw,
                "location_code": 2208,
                "language_code": "da",
                "depth": depth,
            }
            for kw in keywords
        ]

        await _rate_limiter.acquire()
        task_ids = await self._create_task(payload)
        if not task_ids:
            logger.error(f"Failed to create tasks for chunk: {keywords}")
            return {kw: [] for kw in keywords}

        logger.info(f"Created {len(task_ids)} tasks for chunk")

        task_to_keyword = {
            task_id: keywords[i] for i, (task_id, _) in enumerate(task_ids)
        }

        ready_ids = await self._wait_for_tasks_ready_bulk(
            [task_id for task_id, _ in task_ids]
        )

        chunk_items: dict[str, list] = {kw: [] for kw in keywords}

        async def fetch_one(task_id: str):
            keyword = task_to_keyword.get(task_id, "unknown")
            raw = await self._get_task_result(task_id)
            if raw:
                items = self._parse_items(raw)
                logger.info(f"  '{keyword}': {len(items)} results")
                chunk_items[keyword] = items
                del raw  # release large dict immediately
            else:
                logger.error(f"  '{keyword}': failed to fetch results")

        await asyncio.gather(*[fetch_one(tid) for tid in ready_ids.values()])
        return chunk_items

    # ------------------------------------------------------------------
    # PRIVATE — API calls
    # ------------------------------------------------------------------

    async def _create_task(self, payload: list) -> Optional[list]:
        """POST task(s) to DataForSEO and return list of (task_id, cost) tuples."""
        try:
            client = get_http_client()
            response = await client.post(
                f"{API_BASE_URL}/task_post",
                auth=self.auth,
                headers=self.headers,
                content=json.dumps(payload),
                timeout=60.0,
            )
            if response.status_code == 200:
                result = response.json()
                task_ids = [
                    (task.get("id"), task.get("cost", 0))
                    for task in result.get("tasks", [])
                    if task.get("id")
                ]
                logger.info(f"Created {len(task_ids)} task(s)")
                return task_ids or None
            else:
                logger.error(
                    f"Task creation failed — {response.status_code}: {response.text}"
                )
                return None
        except Exception as e:
            logger.error(f"Error creating task: {e}")
            return None

    async def _wait_for_tasks_ready_bulk(
        self,
        task_ids: list[str],
        max_retries: int = 40,
        poll_interval: int = 15,
    ) -> dict[str, str]:
        """Poll tasks_ready until all task_ids in this chunk are ready."""
        pending = set(task_ids)
        ready: dict[str, str] = {}
        client = get_http_client()

        logger.info(f"Polling for {len(pending)} tasks to become ready...")
        await asyncio.sleep(10)  # DataForSEO needs a moment before tasks appear

        for attempt in range(max_retries):
            if not pending:
                break

            try:
                await _rate_limiter.acquire()
                response = await client.get(
                    "https://api.dataforseo.com/v3/serp/tasks_ready",
                    auth=self.auth,
                    headers=self.headers,
                    timeout=30.0,
                )

                if response.status_code == 200:
                    for task in response.json().get("tasks", []):
                        status_code = task.get("status_code", 0)
                        if status_code == 20000 and task.get("result"):
                            for result in task["result"]:
                                result_id = result.get("id")
                                if result_id in pending:
                                    ready[result_id] = result_id
                                    pending.discard(result_id)
                                    logger.info(
                                        f"  Task {result_id} ready "
                                        f"({len(ready)}/{len(task_ids)})"
                                    )
                        elif status_code == 40202:
                            logger.warning(
                                "Rate limit hit while polling — backing off 60s"
                            )
                            await asyncio.sleep(60)
                            break

                # Direct-check stragglers after attempt 2 to handle
                # cases where tasks_ready misses them
                if attempt >= 2:
                    for task_id in list(pending):
                        await _rate_limiter.acquire()
                        direct = await client.get(
                            f"{API_BASE_URL}/task_get/advanced/{task_id}",
                            auth=self.auth,
                            headers=self.headers,
                            timeout=30.0,
                        )
                        if direct.status_code == 200:
                            for task in direct.json().get("tasks", []):
                                if task.get("status_code") == 20000:
                                    ready[task_id] = task_id
                                    pending.discard(task_id)
                                    logger.info(
                                        f"  Task {task_id} ready via direct fetch"
                                    )

            except Exception as e:
                logger.error(f"Error in poll attempt {attempt + 1}: {e}")

            if pending:
                logger.info(
                    f"  Still waiting for {len(pending)} tasks "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(poll_interval)

        if pending:
            logger.warning(f"{len(pending)} tasks never became ready: {pending}")

        return ready

    async def _wait_for_task_ready(
        self,
        task_id: str,
        max_retries: int = 40,
        poll_interval: int = 15,
    ) -> Optional[str]:
        """Single task polling — backwards compatibility wrapper."""
        result = await self._wait_for_tasks_ready_bulk(
            [task_id], max_retries, poll_interval
        )
        return result.get(task_id)

    async def _get_task_result(self, task_id: str) -> Optional[dict]:
        try:
            await _rate_limiter.acquire()
            client = get_http_client()
            response = await client.get(
                f"{API_BASE_URL}/task_get/advanced/{task_id}",
                auth=self.auth,
                headers=self.headers,
                timeout=120.0,
            )
            if response.status_code == 200:
                logger.info(f"Fetched results for task {task_id}")
                return response.json()
            else:
                logger.error(
                    f"Result fetch failed — {response.status_code}: {response.text}"
                )
                return None
        except httpx.TimeoutException as e:
            logger.error(f"Timeout fetching task result: {e}")
            return None
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching task result: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching task result: {e}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # PRIVATE — Parsing
    # ------------------------------------------------------------------

    def _parse_items(self, raw: dict) -> list:
        """Extract and normalise SERP items from the raw DataForSEO response."""
        items = []
        try:
            for task in raw.get("tasks", []):
                for result in task.get("result", []):
                    for item in result.get("items", []):
                        items.append(
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
                        )
        except Exception as e:
            logger.error(f"Error parsing SERP items: {e}")
        return items
