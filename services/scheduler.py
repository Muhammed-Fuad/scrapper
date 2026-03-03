"""
services/scheduler.py

Key fixes:
  - scrape_single_site returns {site_id, site_name, status, total_venues, pages_scraped}
    (NO 'venues' key) — DB saving happens inside scrape_single_site already.
    Scheduler now only handles logging, backup metadata, and status tracking.
  - Removed double DB save (was saving once inside scraper, once again here).
  - Fixed NameError: total_venues was used but never initialized in scrape_all_sites.
  - Added os.makedirs for backups/ directory before writing backup files.
  - scrape_site_and_save no longer tries to access result["venues"].
"""

import asyncio
import logging
import os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from typing import Dict

from utils.scraper_utils import scrape_single_site, scrape_multiple_sites
from config import get_enabled_sites, get_site_by_id, APP_CONFIG
from utils.mongodb_utils import mongodb_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MultiSiteScraperScheduler:
    """Manages scheduled scraping for multiple sites."""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.active_scrapes: Dict[str, bool] = {}
        self.last_scrape_results: Dict[str, dict] = {}

    async def scrape_site_and_save(self, site_id: str):
        """
        Scrape a single site.
        NOTE: DB saving is handled inside scrape_single_site already.
        This method handles scheduling guards, logging, and status tracking only.
        """
        if self.active_scrapes.get(site_id, False):
            logger.warning(f"⚠️ [{site_id}] Already scraping. Skipping...")
            return

        site_config = get_site_by_id(site_id)
        if not site_config or not site_config.enabled:
            logger.warning(f"⚠️ [{site_id}] Site not found or disabled")
            return

        self.active_scrapes[site_id] = True
        log_id = await mongodb_manager.create_scraping_log(site_id, site_config.name)

        try:
            logger.info(f"🚀 [{site_id}] Starting scheduled scrape")

            # scrape_single_site handles fetching, extraction, and DB upsert internally
            result = await scrape_single_site(site_config)

            status = result.get("status", "unknown")
            total_venues = result.get("total_venues", 0)
            pages_scraped = result.get("pages_scraped", 0)

            logger.info(
                f"[{site_id}] Scrape complete — status={status}, "
                f"venues={total_venues}, pages={pages_scraped}"
            )

            if status == "success" and total_venues > 0:
                if log_id:
                    await mongodb_manager.update_scraping_log(log_id, {
                        "status": "success",
                        "venues_found": total_venues,
                        "pages_scraped": pages_scraped,
                    })

                self.last_scrape_results[site_id] = {
                    "timestamp": datetime.utcnow(),
                    "status": "success",
                    "venues_found": total_venues,
                    "pages_scraped": pages_scraped,
                }

            else:
                logger.warning(f"⚠️ [{site_id}] No venues scraped")
                if log_id:
                    await mongodb_manager.update_scraping_log(log_id, {
                        "status": "no_data" if status != "failed" else "failed",
                        "error_message": result.get("error", "No venues found"),
                    })

        except Exception as e:
            logger.error(f"❌ [{site_id}] Scraping error: {e}", exc_info=True)
            if log_id:
                await mongodb_manager.update_scraping_log(log_id, {
                    "status": "failed",
                    "error_message": str(e),
                })

        finally:
            self.active_scrapes[site_id] = False

    async def scrape_all_sites(self):
        """
        Scrape all enabled sites sequentially.
        DB saving happens inside scrape_multiple_sites → scrape_single_site.
        This method aggregates totals and handles cleanup only.
        """
        enabled_sites = get_enabled_sites()

        if not enabled_sites:
            logger.warning("⚠️ No enabled sites to scrape")
            return

        logger.info(f"🌐 Starting scrape for {len(enabled_sites)} sites")

        try:
            results = await scrape_multiple_sites(enabled_sites)

            total_venues = 0  # FIXED: was used but never initialized

            for result in results:
                site_id = result.get("site_id", "unknown")
                status = result.get("status", "unknown")
                count = result.get("total_venues", 0)

                if status == "success" and count > 0:
                    total_venues += count
                    logger.info(f"✅ [{site_id}] {count} venues saved")

                    # Optional JSON backup (metadata only, not re-saving to DB)
                    if APP_CONFIG.backup_enabled:
                        os.makedirs("backups", exist_ok=True)  # FIXED: ensure dir exists
                        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                        backup_meta_file = f"backups/{site_id}_{timestamp}_meta.json"
                        import json
                        with open(backup_meta_file, "w") as f:
                            json.dump(result, f, indent=2, default=str)
                        logger.info(f"[{site_id}] Backup metadata saved: {backup_meta_file}")

                else:
                    logger.warning(f"⚠️ [{site_id}] status={status}, venues={count}")

            logger.info(f"📊 Total venues saved across all sites: {total_venues}")

            # Cleanup stale venues older than configured threshold
            deleted = await mongodb_manager.delete_old_venues(
                days=APP_CONFIG.cleanup_old_venues_days
            )
            logger.info(f"🧹 Cleaned up {deleted} old venues")

        except Exception as e:
            logger.error(f"❌ Error in scrape_all_sites: {e}", exc_info=True)

    def start(self):
        """Start the scheduler with per-site intervals."""
        logger.info("🕒 Starting multi-site scheduler")

        enabled_sites = get_enabled_sites()

        # Run an initial full scrape immediately on startup
        self.scheduler.add_job(
            self.scrape_all_sites,
            trigger="date",
            id="initial_scrape_all",
            name="Initial scrape all sites",
        )

        # Schedule each site on its own interval
        for site in enabled_sites:
            self.scheduler.add_job(
                self.scrape_site_and_save,
                trigger=IntervalTrigger(hours=site.scrape_interval_hours),
                args=[site.site_id],
                id=f"scrape_{site.site_id}",
                name=f"Scrape {site.name}",
                replace_existing=True,
            )
            logger.info(
                f"📅 Scheduled [{site.site_id}] '{site.name}' "
                f"every {site.scrape_interval_hours}h"
            )

        self.scheduler.start()
        logger.info("✅ Scheduler started")

    def stop(self):
        """Stop the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("⏹️ Scheduler stopped")

    def get_status(self) -> dict:
        """Return current scheduler and job status."""
        jobs = self.scheduler.get_jobs()
        return {
            "is_running": self.scheduler.running,
            "active_scrapes": self.active_scrapes,
            "last_results": {
                k: {**v, "timestamp": v["timestamp"].isoformat()}
                for k, v in self.last_scrape_results.items()
            },
            "scheduled_jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": str(job.next_run_time),
                }
                for job in jobs
            ],
        }


# Global scheduler instance
multi_site_scheduler = MultiSiteScraperScheduler()