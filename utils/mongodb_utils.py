"""
utils/mongodb_utils.py

Key fixes:
  - upsert_venues_batch now uses bulk_write (single round-trip) instead of
    calling upsert_venue in a loop (was N×2 round-trips — find + update per venue).
  - All datetime fields stored as actual datetime objects consistently.
    Previously scraper_utils wrote ISO strings while mongodb_utils wrote datetimes,
    causing delete_old_venues $lt comparison to silently fail.
  - disconnect() uncommented — was leaking Motor connections on shutdown.
  - Removed index on 'is_free' (field doesn't exist in any model).
  - Added text index on name+description+location for proper full-text search.
  - search_venues now uses $text search instead of per-field regex (uses the index).
  - Added Motor connection pool settings suitable for a scraper workload.
  - upsert_venue kept for single-item use but batch path bypasses it entirely.
"""

import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import logging

logger = logging.getLogger(__name__)


class MongoDBManager:
    """Manages MongoDB connection and operations."""

    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.venues_collection = None
        self.logs_collection = None
        self.mongo_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
        self.db_name = os.getenv("MONGODB_DB", "Planora")

    async def connect(self):
        """Connect to MongoDB with tuned connection pool."""
        try:
            self.client = AsyncIOMotorClient(
                self.mongo_url,
                maxPoolSize=20,       # Allow more concurrent operations
                minPoolSize=2,        # Keep a few connections warm
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            self.db = self.client[self.db_name]
            self.venues_collection = self.db["venues"]
            self.logs_collection = self.db["scraping_logs"]

            await self._create_indexes()
            logger.info(f"✅ Connected to MongoDB: {self.db_name}")

        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise

    async def _create_indexes(self):
        """
        Ensure indexes exist. Each create_index call is wrapped individually
        so a conflict on one index (e.g. name mismatch with an existing index)
        is logged and skipped rather than crashing the whole app.
        """
        from pymongo.errors import OperationFailure

        indexes = [
            # (collection, args, kwargs)
            (self.venues_collection, [("name", 1), ("date", 1), ("source_site_id", 1)], {"unique": True}),
            (self.venues_collection, "source_site_id", {}),
            (self.venues_collection, "location", {}),
            (self.venues_collection, "category", {}),
            (self.venues_collection, "last_scraped", {}),
            (self.venues_collection, "created_at", {}),
            (self.venues_collection, "rate", {}),
            (self.venues_collection, [("name", "text"), ("description", "text"), ("location", "text")],
                {"weights": {"name": 10, "location": 5, "description": 1}}),
            (self.logs_collection, "site_id", {}),
            (self.logs_collection, "started_at", {}),
            (self.logs_collection, [("site_id", 1), ("started_at", -1)], {}),
        ]

        for collection, keys, kwargs in indexes:
            try:
                await collection.create_index(keys, **kwargs)
            except OperationFailure as e:
                # Code 85 = IndexOptionsConflict (index exists with different name/options)
                # Code 86 = IndexKeySpecsConflict
                # Safe to skip — existing index already covers this need
                logger.warning(f"⚠️  Index already exists, skipping: {e.details.get("errmsg", str(e))}")
            except Exception as e:
                logger.error(f"❌ Unexpected error creating index on {collection.name}: {e}")

        logger.info("📑 MongoDB indexes ensured")

    async def disconnect(self):
        """Disconnect from MongoDB and release the connection pool."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("🔌 MongoDB connection closed")

    # ==================== VENUE OPERATIONS ====================

    async def upsert_venue(self, venue: dict, site_id: str) -> tuple[bool, str]:
        """
        Insert or update a single venue.
        For batch operations use upsert_venues_batch (much faster).
        Returns (is_new, operation_type).
        """
        try:
            now = datetime.utcnow()
            venue["source_site_id"] = site_id
            venue["last_scraped"] = now   # Store as datetime, not ISO string

            filter_doc = {
                "name": venue["name"],
                "source_site_id": site_id,
            }

            update_doc = {
                "$set": {**venue, "updated_at": now},
                "$setOnInsert": {"created_at": now},
            }

            result = await self.venues_collection.update_one(
                filter_doc, update_doc, upsert=True
            )

            if result.upserted_id:
                return True, "created"
            elif result.modified_count:
                return False, "updated"
            else:
                return False, "unchanged"

        except Exception as e:
            logger.error(f"❌ Error upserting venue '{venue.get('name')}': {e}")
            return False, "error"

    async def upsert_venues_batch(
        self, venues: List[dict], site_id: str, site_name: str
    ) -> Dict[str, int]:
        """
        Batch upsert venues using a single bulk_write call.

        FIXED: Original looped upsert_venue() — that's N×2 round-trips (find + update
        per venue). bulk_write sends everything in one request regardless of batch size.
        """
        if not venues:
            return {"created": 0, "updated": 0, "errors": 0, "total": 0}

        now = datetime.utcnow()
        operations = []

        for venue in venues:
            # Strip fields that must NOT appear in both $set and $setOnInsert
            # (MongoDB rejects documents where the same field appears in both operators)
            # Also strip _id to avoid immutable field errors on existing documents
            set_doc = {
                k: v for k, v in venue.items()
                if k not in ("_id", "created_at")
            }

            # Always overwrite these with fresh datetime objects (not ISO strings)
            set_doc["source_site_id"] = site_id
            set_doc["source_site_name"] = site_name
            set_doc["last_scraped"] = now
            set_doc["updated_at"] = now

            operations.append(
                UpdateOne(
                    filter={
                        "name": venue["name"],
                        "source_site_id": site_id,
                    },
                    update={
                        "$set": set_doc,
                        "$setOnInsert": {"created_at": now},  # Only set on new insert
                    },
                    upsert=True,
                )
            )

        created = 0
        updated = 0
        errors = 0

        try:
            result = await self.venues_collection.bulk_write(
                operations,
                ordered=False,  # Continue on error, maximize throughput
            )
            created = result.upserted_count
            updated = result.modified_count

        except BulkWriteError as bwe:
            # Partial success — count what succeeded
            details = bwe.details
            created = details.get("nUpserted", 0)
            updated = details.get("nModified", 0)
            errors = len(details.get("writeErrors", []))
            logger.warning(
                f"[{site_id}] Bulk write partial failure: "
                f"{errors} errors out of {len(venues)} venues"
            )
            for err in details.get("writeErrors", [])[:5]:  # Log first 5 for debugging
                logger.error(f"  Write error code={err.get('code')} msg={err.get('errmsg')}")

        except Exception as e:
            logger.error(f"❌ Bulk write failed for {site_id}: {e}")
            errors = len(venues)

        result_summary = {
            "created": created,
            "updated": updated,
            "errors": errors,
            "total": len(venues),
        }
        logger.info(f"📊 [{site_id}] Batch upsert: {result_summary}")
        return result_summary

    async def get_all_venues(
        self,
        skip: int = 0,
        limit: int = 100,
        site_id: Optional[str] = None,
    ) -> List[dict]:
        """Get venues with optional site filter, sorted by newest first."""
        try:
            query = {}
            if site_id:
                query["source_site_id"] = site_id

            cursor = (
                self.venues_collection.find(query, {"_id": 0})
                .sort("created_at", -1)
                .skip(skip)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)

        except Exception as e:
            logger.error(f"❌ Error fetching venues: {e}")
            return []

    async def get_venue_count(self, site_id: Optional[str] = None) -> int:
        """Get total venue count, optionally filtered by site."""
        try:
            query = {"source_site_id": site_id} if site_id else {}
            return await self.venues_collection.count_documents(query)
        except Exception as e:
            logger.error(f"❌ Error counting venues: {e}")
            return 0

    async def search_venues(
        self,
        query: str,
        skip: int = 0,
        limit: int = 20,
        site_id: Optional[str] = None,
    ) -> List[dict]:
        """
        Full-text search using MongoDB $text index (name + description + location).
        FIXED: Original used per-field regex — unindexed, slow on large collections.
        Text index gives relevance-scored, indexed search.
        """
        try:
            search_filter: dict = {"$text": {"$search": query}}
            if site_id:
                search_filter["source_site_id"] = site_id

            cursor = (
                self.venues_collection.find(
                    search_filter,
                    {
                        "_id": 0,
                        "score": {"$meta": "textScore"},  # Relevance score
                    },
                )
                .sort([("score", {"$meta": "textScore"})])
                .skip(skip)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)

        except Exception as e:
            logger.error(f"❌ Error searching venues: {e}")
            return []

    async def delete_old_venues(
        self, days: int = 30, site_id: Optional[str] = None
    ) -> int:
        """
        Delete venues not scraped in the last N days.
        FIXED: Now works correctly because last_scraped is stored as datetime
        (was broken when scraper stored ISO strings and this compared with datetime).
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query: dict = {"last_scraped": {"$lt": cutoff_date}}
            if site_id:
                query["source_site_id"] = site_id

            result = await self.venues_collection.delete_many(query)
            logger.info(
                f"🗑️ Deleted {result.deleted_count} venues "
                f"older than {days} days from {site_id or 'all sites'}"
            )
            return result.deleted_count

        except Exception as e:
            logger.error(f"❌ Error deleting old venues: {e}")
            return 0

    async def get_venues_by_location(
        self, location: str, skip: int = 0, limit: int = 50
    ) -> List[dict]:
        """Get venues by location using case-insensitive regex."""
        try:
            cursor = (
                self.venues_collection.find(
                    {"location": {"$regex": location, "$options": "i"}},
                    {"_id": 0},
                )
                .skip(skip)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)

        except Exception as e:
            logger.error(f"❌ Error fetching venues by location: {e}")
            return []

    # ==================== SCRAPING LOG OPERATIONS ====================

    async def create_scraping_log(self, site_id: str, site_name: str) -> Optional[str]:
        """Create a new scraping log entry, return its ID."""
        try:
            log = {
                "site_id": site_id,
                "site_name": site_name,
                "started_at": datetime.utcnow(),
                "status": "running",
                "venues_found": 0,
                "venues_created": 0,
                "venues_updated": 0,
                "errors": 0,
            }
            result = await self.logs_collection.insert_one(log)
            return str(result.inserted_id)

        except Exception as e:
            logger.error(f"❌ Error creating scraping log: {e}")
            return None

    async def update_scraping_log(self, log_id: str, update_data: dict):
        """Update a scraping log entry with results and duration."""
        try:
            from bson import ObjectId

            now = datetime.utcnow()
            update_data["completed_at"] = now

            log = await self.logs_collection.find_one({"_id": ObjectId(log_id)})
            if log and "started_at" in log:
                update_data["duration_seconds"] = (
                    now - log["started_at"]
                ).total_seconds()

            await self.logs_collection.update_one(
                {"_id": ObjectId(log_id)},
                {"$set": update_data},
            )

        except Exception as e:
            logger.error(f"❌ Error updating scraping log {log_id}: {e}")

    async def get_scraping_logs(
        self, site_id: Optional[str] = None, limit: int = 50
    ) -> List[dict]:
        """Get scraping logs, newest first."""
        try:
            query = {"site_id": site_id} if site_id else {}
            cursor = (
                self.logs_collection.find(query, {"_id": 0})
                .sort("started_at", -1)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)

        except Exception as e:
            logger.error(f"❌ Error fetching logs: {e}")
            return []

    async def get_site_statistics(self, site_id: str) -> dict:
        """Get aggregated statistics for a specific site."""
        try:
            total_venues = await self.get_venue_count(site_id=site_id)

            last_log = await self.logs_collection.find_one(
                {"site_id": site_id, "status": "success"},
                sort=[("completed_at", -1)],
            )

            recent_logs = await self.get_scraping_logs(site_id=site_id, limit=10)

            last_scrape_str = None
            if last_log and last_log.get("completed_at"):
                ts = last_log["completed_at"]
                last_scrape_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)

            return {
                "site_id": site_id,
                "total_venues": total_venues,
                "last_scrape": last_scrape_str,
                "last_scrape_result": {
                    "venues_found": last_log.get("venues_found", 0),
                    "venues_created": last_log.get("venues_created", 0),
                    "venues_updated": last_log.get("venues_updated", 0),
                    "duration_seconds": last_log.get("duration_seconds"),
                }
                if last_log
                else None,
                "recent_scrapes": len(recent_logs),
                "recent_logs": recent_logs[:5],
            }

        except Exception as e:
            logger.error(f"❌ Error getting site statistics for {site_id}: {e}")
            return {}


# ==================== GLOBAL INSTANCE ====================
mongodb_manager = MongoDBManager()