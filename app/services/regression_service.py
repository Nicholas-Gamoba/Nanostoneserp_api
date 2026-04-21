import logging
import datetime
import asyncpg
from app.config import settings

logger = logging.getLogger(__name__)

RANK_CHANGE_THRESHOLD = 5
RECHECK_DELAY_HOURS = 3


async def _get_db():
    return await asyncpg.connect(settings.DATABASE_URL)


async def queue_regressions() -> dict:
    conn = await _get_db()
    try:
        dates = await conn.fetch(
            "SELECT DISTINCT serp_date FROM serp_results ORDER BY serp_date DESC LIMIT 2"
        )
        if len(dates) < 2:
            return {"status": "skipped", "reason": "insufficient_history"}

        latest_date = dates[0]["serp_date"]
        prev_date = dates[1]["serp_date"]
        logger.info(f"Regression check: comparing {prev_date} → {latest_date}")

        own_domains = await conn.fetch('SELECT domain FROM "OwnSite"')
        if not own_domains:
            return {"status": "skipped", "reason": "no_own_sites"}

        domain_list = [r["domain"] for r in own_domains]

        latest_ranks = await conn.fetch(
            """
            SELECT keyword_id, MIN(rank_group) AS rank_group
            FROM serp_results
            WHERE serp_date = $1 AND domain = ANY($2) AND type = 'organic'
            GROUP BY keyword_id
            """,
            latest_date, domain_list,
        )

        prev_ranks = await conn.fetch(
            """
            SELECT sr.keyword_id, sk.keyword, MIN(sr.rank_group) AS rank_group
            FROM serp_results sr
            JOIN "SEOKeyword" sk ON sk.id = sr.keyword_id
            WHERE sr.serp_date = $1 AND sr.domain = ANY($2) AND sr.type = 'organic'
            GROUP BY sr.keyword_id, sk.keyword
            """,
            prev_date, domain_list,
        )

        latest_map = {r["keyword_id"]: r["rank_group"] for r in latest_ranks}
        prev_map = {
            r["keyword_id"]: {"rank": r["rank_group"], "keyword": r["keyword"]}
            for r in prev_ranks
        }

        run_after = datetime.datetime.now(datetime.timezone.utc) + \
                    datetime.timedelta(hours=RECHECK_DELAY_HOURS)

        lost, moved = [], []

        for kw_id, prev_data in prev_map.items():
            prev_rank = prev_data["rank"]
            keyword = prev_data["keyword"]
            new_rank = latest_map.get(kw_id)

            reason = None
            if new_rank is None:
                reason = "lost"
                lost.append(keyword)
            elif abs(new_rank - prev_rank) > RANK_CHANGE_THRESHOLD:
                reason = "moved_5"
                moved.append(f"{keyword} ({prev_rank}→{new_rank})")

            if reason:
                await conn.execute(
                    """
                    INSERT INTO serp_recheck_queue
                        (keyword_id, keyword, trigger_reason, prev_rank, run_after, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    ON CONFLICT (keyword_id, status) DO UPDATE SET
                        trigger_reason = EXCLUDED.trigger_reason,
                        prev_rank      = EXCLUDED.prev_rank,
                        run_after      = EXCLUDED.run_after,
                        created_at     = NOW()
                    """,
                    kw_id, keyword, reason, prev_rank, run_after,
                )

        logger.info(f"Regressions queued — lost: {len(lost)}, moved >5: {len(moved)}")
        return {
            "status": "ok",
            "compared": f"{prev_date} → {latest_date}",
            "lost": lost,
            "moved_5": moved,
            "recheck_after": run_after.isoformat() if (lost or moved) else None, 
        }
    finally:
        await conn.close()


async def get_due_rechecks() -> list[dict]:
    """Items whose run_after has passed — marks them done atomically."""
    conn = await _get_db()
    try:
        rows = await conn.fetch(
            """
            UPDATE serp_recheck_queue
            SET status = 'done'
            WHERE status = 'pending' AND run_after <= NOW()
            RETURNING id, keyword_id, keyword
            """
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def get_pending_rechecks() -> list[dict]:
    """Items queued but not yet due — used for restart recovery."""
    conn = await _get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT id, keyword_id, keyword, run_after
            FROM serp_recheck_queue
            WHERE status = 'pending' AND run_after > NOW()
            """
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()