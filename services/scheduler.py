# services/scheduler.py
import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from typing import Dict

from utils.scraper_utils import scrape_single_site, scrape_multiple_sites
from config import get_enabled_sites, get_site_by_id, APP_CONFIG
from utils.mongodb_utils import mongodb_manager
from utils.data_utils import save_venues_to_json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MultiSiteScraperScheduler:
    """Manages scheduled scraping for multiple sites."""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.active_scrapes: Dict[str, bool] = {}
        self.last_scrape_results: Dict[str, dict] = {}
    
    async def scrape_site_and_save(self, site_id: str):
        """Scrape a single site and save to database."""
        
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
            
            # Scrape the site
            result = await scrape_single_site(site_config)
            
            logger.info(f"[{site_id}] Scrape result: status={result['status']}, venues={result['total_venues']}")
            
            if result["status"] == "success" and result["venues"]:
                # Log sample venue
                if result["venues"]:
                    sample = result["venues"][0]
                    logger.info(f"[{site_id}] Sample venue: {sample.get('name', 'N/A')} | {sample.get('location', 'N/A')}")
                
                # Save backup
                if APP_CONFIG.backup_enabled:
                    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                    backup_file = f"backups/{site_id}_{timestamp}.json"
                    save_venues_to_json(result["venues"], backup_file)
                    logger.info(f"[{site_id}] Backup saved: {backup_file}")
                
                # Save to MongoDB
                logger.info(f"[{site_id}] Saving {len(result['venues'])} venues to MongoDB...")
                db_result = await mongodb_manager.upsert_venues_batch(
                    venues=result["venues"],
                    site_id=site_id,
                    site_name=site_config.name
                )
                
                logger.info(
                    f"✅ [{site_id}] Database save complete: "
                    f"Created={db_result['created']}, Updated={db_result['updated']}, Errors={db_result['errors']}"
                )
                
                # Update log
                if log_id:
                    await mongodb_manager.update_scraping_log(log_id, {
                        "status": "success",
                        "venues_found": result["total_venues"],
                        "venues_created": db_result["created"],
                        "venues_updated": db_result["updated"],
                        "errors": db_result["errors"]
                    })
                
                # Store result
                self.last_scrape_results[site_id] = {
                    "timestamp": datetime.utcnow(),
                    "status": "success",
                    "venues_found": result["total_venues"],
                    "created": db_result["created"],
                    "updated": db_result["updated"]
                }
                
            else:
                # Failed or no venues
                logger.warning(f"⚠️ [{site_id}] No venues scraped")
                if log_id:
                    await mongodb_manager.update_scraping_log(log_id, {
                        "status": "failed" if result["status"] == "failed" else "no_data",
                        "error_message": result.get("error", "No venues found")
                    })
            
        except Exception as e:
            logger.error(f"❌ [{site_id}] Scraping error: {e}", exc_info=True)
            
            if log_id:
                await mongodb_manager.update_scraping_log(log_id, {
                    "status": "failed",
                    "error_message": str(e)
                })
        
        finally:
            self.active_scrapes[site_id] = False
    
    async def scrape_all_sites(self):
        """Scrape all enabled sites concurrently."""
        enabled_sites = get_enabled_sites()
        
        if not enabled_sites:
            logger.warning("⚠️ No enabled sites to scrape")
            return
        
        logger.info(f"🌐 Starting scrape for {len(enabled_sites)} sites")
        
        try:
            # Scrape all sites
            results = await scrape_multiple_sites(enabled_sites)

            
            total_created = 0
            total_updated = 0
            
            # Save results to database
            for result in results:
                venues = result.get("venues", [])
                if result.get("status") == "success" and len(venues) > 0:
                    total_venues += len(venues)

                    site_id = result["site_id"]
                    
                    logger.info(f"[{site_id}] Processing {len(result['venues'])} venues...")
                    
                    # Save backup
                    if APP_CONFIG.backup_enabled:
                        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                        backup_file = f"backups/{site_id}_{timestamp}.json"
                        save_venues_to_json(result["venues"], backup_file)
                    
                    # Save to MongoDB
                    db_result = await mongodb_manager.upsert_venues_batch(
                        venues=result["venues"],
                        site_id=site_id,
                        site_name=result["site_name"]
                    )
                    
                    total_created += db_result["created"]
                    total_updated += db_result["updated"]
                    
                    logger.info(
                        f"✅ [{site_id}] Saved: "
                        f"{db_result['created']} created, {db_result['updated']} updated"
                    )
            
            logger.info(f"📊 Total: {total_created} created, {total_updated} updated")
            
            # Cleanup old venues
            deleted = await mongodb_manager.delete_old_venues(
                days=APP_CONFIG.cleanup_old_venues_days
            )
            logger.info(f"🧹 Cleaned up {deleted} old venues")
            
        except Exception as e:
            logger.error(f"❌ Error in scrape_all_sites: {e}", exc_info=True)
    
    def start(self):
        """Start the scheduler."""
        logger.info("🕒 Starting multi-site scheduler")
        
        enabled_sites = get_enabled_sites()
        
        # Schedule initial scrape
        self.scheduler.add_job(
            self.scrape_all_sites,
            trigger="date",
            id="initial_scrape_all",
            name="Initial scrape all sites"
        )
        
        # Schedule individual sites
        for site in enabled_sites:
            self.scheduler.add_job(
                self.scrape_site_and_save,
                trigger=IntervalTrigger(hours=site.scrape_interval_hours),
                args=[site.site_id],
                id=f"scrape_{site.site_id}",
                name=f"Scrape {site.name}",
                replace_existing=True
            )
            
            logger.info(f"📅 Scheduled {site.name} every {site.scrape_interval_hours} hours")
        
        self.scheduler.start()
        logger.info("✅ Scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("⏹️ Scheduler stopped")
    
    def get_status(self) -> dict:
        """Get scheduler status."""
        jobs = self.scheduler.get_jobs()
        
        return {
            "is_running": self.scheduler.running,
            "active_scrapes": self.active_scrapes,
            "last_results": self.last_scrape_results,
            "scheduled_jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time
                }
                for job in jobs
            ]
        }


# Global scheduler instance
multi_site_scheduler = MultiSiteScraperScheduler()