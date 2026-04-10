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


class RateLimiter:
    """Sliding window rate limiter — max N requests per 60 seconds"""

    def __init__(self, max_per_minute: int = 18):
        self.max_per_minute = max_per_minute
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = datetime.utcnow()
            # Drop timestamps older than 60s
            while self._timestamps and self._timestamps[0] < now - timedelta(
                seconds=60
            ):
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_per_minute:
                # Wait until the oldest request falls outside the window
                wait_until = self._timestamps[0] + timedelta(seconds=60)
                wait_seconds = (wait_until - now).total_seconds() + 0.5
                logger.info(f"Rate limit reached — waiting {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                # Re-clean after sleep
                now = datetime.utcnow()
                while self._timestamps and self._timestamps[0] < now - timedelta(
                    seconds=60
                ):
                    self._timestamps.popleft()

            self._timestamps.append(datetime.utcnow())


# Module-level singleton — shared across all SerpService instances
_rate_limiter = RateLimiter(max_per_minute=18)


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
        """Full flow: create task → wait for ready → fetch results → parse items"""

        payload = [
            {
                "keyword": keyword,
                "location_code": 2208,
                "language_code": "da",
                "depth": depth,
            }
        ]

        # Acquire rate limit slot before hitting the API
        await _rate_limiter.acquire()
        task_ids = await self._create_task(payload)
        if not task_ids:
            raise RuntimeError(
                f"Failed to create DataForSEO task for keyword: '{keyword}'"
            )

        task_id, cost = task_ids[0]
        logger.info(f"Task created — id: {task_id}, estimated cost: ${cost}")

        permanent_id = await self._wait_for_task_ready(task_id)
        if not permanent_id:
            raise RuntimeError(f"Task {task_id} never became ready")

        raw = await self._get_task_result(permanent_id)
        if not raw:
            raise RuntimeError(f"Failed to fetch results for task {permanent_id}")

        items = self._parse_items(raw)
        logger.info(f"Parsed {len(items)} SERP items for keyword: '{keyword}'")
        return items

    # ------------------------------------------------------------------
    # PRIVATE — API calls
    # ------------------------------------------------------------------

    async def _create_task(self, payload: list) -> Optional[list]:
        """POST task to DataForSEO and return list of (task_id, cost) tuples"""
        try:
            async with httpx.AsyncClient() as client:
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

    async def _wait_for_task_ready(
        self,
        task_id: str,
        max_retries: int = 40,
        poll_interval: int = 15,
    ) -> Optional[str]:
        """Poll tasks_ready until our task_id appears, return permanent id"""
        logger.info(f"Waiting for task {task_id} to be ready...")
        await asyncio.sleep(10)

        for attempt in range(max_retries):
            try:
                # Rate limit the polling calls too
                await _rate_limiter.acquire()
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        "https://api.dataforseo.com/v3/serp/tasks_ready",
                        auth=self.auth,
                        headers=self.headers,
                        timeout=30.0,
                    )

                if response.status_code == 200:
                    tasks = response.json().get("tasks", [])

                    for task in tasks:
                        status_code = task.get("status_code", 0)

                        if status_code == 20000 and task.get("result"):
                            for result in task["result"]:
                                result_id = result.get("id")
                                if result_id == task_id:
                                    logger.info(f"Task {task_id} is ready")
                                    return result_id

                        elif status_code == 40202:
                            # Rate limit error — back off and retry instead of failing
                            logger.warning(
                                f"Rate limit hit while polling — backing off 60s"
                            )
                            await asyncio.sleep(60)
                            break

                        elif status_code >= 40000:
                            logger.error(
                                f"Task failed with status {status_code}: {task.get('status_message')}"
                            )
                            # Don't return None immediately — the error may be on a
                            # different task in the list, not ours
                            continue

                    logger.info(
                        f"Task {task_id} not ready yet (attempt {attempt + 1}/{max_retries})"
                    )

                else:
                    logger.warning(
                        f"tasks_ready poll failed — {response.status_code}: {response.text}"
                    )

            except Exception as e:
                logger.error(f"Error polling tasks_ready: {e}")

            await asyncio.sleep(poll_interval)

        logger.error(
            f"Task {task_id} did not become ready after {max_retries} attempts"
        )
        return None

    async def _get_task_result(self, task_id: str) -> Optional[dict]:
        try:
            await _rate_limiter.acquire()
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{API_BASE_URL}/task_get/advanced/{task_id}",
                    auth=self.auth,
                    headers=self.headers,
                    timeout=30.0,
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
        """Extract and normalise SERP items from the raw DataForSEO response"""
        items = []

        try:
            tasks = raw.get("tasks", [])
            for task in tasks:
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
                                "images": item.get("images"),
                                "data": item,
                            }
                        )
        except Exception as e:
            logger.error(f"Error parsing SERP items: {e}")

        return items
