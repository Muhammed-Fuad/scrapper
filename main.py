"""
main.py

Key fixes:
  - /venues/search registered BEFORE /venues/location/{location} to prevent FastAPI
    from matching "search" as the {location} path parameter (caused 500 on search).
  - datetime.utcnow() wrapped in .isoformat() in all JSON responses — FastAPI's default
    JSONResponse can't serialize raw datetime objects, causing intermittent 500s.
  - CORS allow_origins reads from CORS_ORIGINS env var (comma-separated) with a
    localhost fallback — avoids hardcoded values breaking in staging/production.
  - search_venues was called with kwarg q= but function signature uses query= — fixed.
  - /venues default limit reduced to 100 (was 1000 with no server-side cap).
"""

import os
import sys
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# ================= WINDOWS EVENT LOOP FIX =================
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
# ==========================================================

from services.scheduler import multi_site_scheduler
from utils.mongodb_utils import mongodb_manager
from config import SCRAPING_SITES, get_enabled_sites, get_site_by_id

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("main")


# ================= LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Multi-Site Venue Scraper API...")

    os.makedirs("logs", exist_ok=True)
    os.makedirs("backups", exist_ok=True)

    await mongodb_manager.connect()
    logger.info(f"🧪 Mongo client id: {id(mongodb_manager.client)}")

    multi_site_scheduler.start()
    logger.info("✅ Scheduler started")

    yield

    logger.info("⏹️ Shutting down...")
    multi_site_scheduler.stop()
    await mongodb_manager.disconnect()
    logger.info("👋 Application stopped")


# ================= APP =================
app = FastAPI(
    title="Multi-Site Venue Scraper API",
    description="Scrapes events from multiple sites and stores them in MongoDB",
    version="2.0.0",
    lifespan=lifespan,
)

# ================= CORS =================
# FIXED: Read from env var so staging/prod don't need code changes
# Set CORS_ORIGINS="https://yourapp.com,https://staging.yourapp.com" in .env
_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================= ROOT =================
@app.get("/")
async def root():
    return {
        "message": "Multi-Site Venue Scraper API",
        "status": "running",
        "version": "2.0.0",
        "sites": len(SCRAPING_SITES),
        "enabled_sites": len(get_enabled_sites()),
        "docs": "/docs",
    }


# ================= HEALTH =================
@app.get("/health")
async def health():
    try:
        total = await mongodb_manager.get_venue_count()
        scheduler_status = multi_site_scheduler.get_status()

        return {
            "status": "healthy",
            "database": {
                "connected": mongodb_manager.client is not None,
                "total_venues": total,
            },
            "scheduler": scheduler_status,
            "timestamp": datetime.utcnow().isoformat(),  # FIXED: serialize to string
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)},
        )


# ================= SITES =================
@app.get("/sites")
async def list_sites():
    return {
        "total": len(SCRAPING_SITES),
        "enabled": len(get_enabled_sites()),
        "sites": [
            {
                "site_id": s.site_id,
                "name": s.name,
                "base_url": s.base_url,
                "enabled": s.enabled,
                "interval_hours": s.scrape_interval_hours,
                "category": s.category,
                "location": s.location,
            }
            for s in SCRAPING_SITES
        ],
    }


@app.get("/sites/{site_id}")
async def site_info(site_id: str = Path(...)):
    site = get_site_by_id(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    stats = await mongodb_manager.get_site_statistics(site_id)

    return {
        "site_id": site.site_id,
        "name": site.name,
        "base_url": site.base_url,
        "enabled": site.enabled,
        "interval_hours": site.scrape_interval_hours,
        "category": site.category,
        "location": site.location,
        "statistics": stats,
    }


# ================= VENUES =================
# CRITICAL: /venues/search MUST be registered before /venues/location/{location}
# FastAPI matches routes top-to-bottom; if the parameterized route comes first,
# "search" gets captured as the {location} value and search never works.

@app.get("/venues/search")
async def search_venues(
    q: str = Query(..., min_length=2, description="Search term"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    site_id: Optional[str] = Query(None),
):
    try:
        # FIXED: was called as search_venues(q=q, ...) but signature uses 'query'
        venues = await mongodb_manager.search_venues(
            query=q, skip=skip, limit=limit, site_id=site_id
        )
        return {
            "query": q,
            "count": len(venues),
            "venues": venues,
        }
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/venues/location/{location}")
async def venues_by_location(
    location: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    venues = await mongodb_manager.get_venues_by_location(
        location, skip=skip, limit=limit
    )
    return {
        "location": location,
        "count": len(venues),
        "venues": venues,
    }


@app.get("/venues")
async def list_venues(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),  # FIXED: reduced default+cap (was 1000/1000)
    site_id: Optional[str] = Query(None),
):
    try:
        venues = await mongodb_manager.get_all_venues(
            skip=skip, limit=limit, site_id=site_id
        )
        total = await mongodb_manager.get_venue_count(site_id)

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "count": len(venues),
            "site_filter": site_id,
            "venues": venues,
        }
    except Exception as e:
        logger.error(f"Venue fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ================= SCRAPING =================
@app.post("/scrape/trigger")
async def trigger_all_scrapes():
    import asyncio
    asyncio.create_task(multi_site_scheduler.scrape_all_sites())
    return {
        "status": "started",
        "timestamp": datetime.utcnow().isoformat(),  # FIXED: serialize to string
    }


@app.post("/scrape/{site_id}/trigger")
async def trigger_site(site_id: str):
    site = get_site_by_id(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    import asyncio
    asyncio.create_task(multi_site_scheduler.scrape_site_and_save(site_id))
    return {
        "site_id": site_id,
        "status": "started",
        "timestamp": datetime.utcnow().isoformat(),  # FIXED: serialize to string
    }


@app.get("/scrape/status")
async def scrape_status():
    return multi_site_scheduler.get_status()


# ================= CLEANUP =================
@app.delete("/cleanup")
async def cleanup(
    days: int = Query(30, ge=1, le=365),
    site_id: Optional[str] = Query(None),
):
    deleted = await mongodb_manager.delete_old_venues(days, site_id)
    return {
        "deleted": deleted,
        "days": days,
        "site_id": site_id,
        "timestamp": datetime.utcnow().isoformat(),  # FIXED: serialize to string
    }


# ================= RUN =================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=False,
        log_level="info",
    )