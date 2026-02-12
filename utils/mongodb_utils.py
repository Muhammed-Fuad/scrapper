# utils/mongodb_utils.py
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
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
        """Connect to MongoDB."""
        try:
            self.client = AsyncIOMotorClient(self.mongo_url)
            self.db = self.client[self.db_name]
            self.venues_collection = self.db["venues"]
            self.logs_collection = self.db["scraping_logs"]
            
            # Create indexes for venues
            await self.venues_collection.create_index(
                [("name", 1), ("date", 1), ("source_site_id", 1)],
                unique=True
            )

            await self.venues_collection.create_index("source_site_id")
            await self.venues_collection.create_index("location")
            await self.venues_collection.create_index("category")
            await self.venues_collection.create_index("last_scraped")
            await self.venues_collection.create_index("created_at")
            await self.venues_collection.create_index("rate")
            await self.venues_collection.create_index("is_free")

            
            # Create indexes for logs
            await self.logs_collection.create_index("site_id")
            await self.logs_collection.create_index("started_at")
            await self.logs_collection.create_index([("site_id", 1), ("started_at", -1)])
            
            logger.info(f"✅ Connected to MongoDB: {self.db_name}")
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB."""
        # if self.client:
        #     self.client.close()
        #     logger.info("MongoDB connection closed")
    
    # ==================== VENUE OPERATIONS ====================
    
    async def upsert_venue(self, venue: dict, site_id: str) -> tuple[bool, str]:
        """
        Insert or update a single venue.
        Returns (is_new, operation_type)
        """
        try:
            venue['source_site_id'] = site_id
            venue['last_scraped'] = datetime.utcnow()
            
            # Check if venue exists
            existing = await self.venues_collection.find_one({
                "name": venue["name"],
                "source_site_id": site_id
            })
            
            if existing:
                # Update existing
                venue['updated_at'] = datetime.utcnow()
                venue['created_at'] = existing.get('created_at', datetime.utcnow())
                
                await self.venues_collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": venue}
                )
                return False, "updated"
            else:
                # Insert new
                venue['created_at'] = datetime.utcnow()
                venue['updated_at'] = datetime.utcnow()
                await self.venues_collection.insert_one(venue)
                return True, "created"
                
        except Exception as e:
            logger.error(f"❌ Error upserting venue: {e}")
            return False, "error"
    
    async def upsert_venues_batch(self, venues: List[dict], site_id: str, site_name: str) -> Dict[str, int]:
        """Batch upsert venues from a specific site."""
        created = 0
        updated = 0
        errors = 0
        
        for venue in venues:
            # Add source metadata
            venue['source_site_id'] = site_id
            venue['source_site_name'] = site_name
            
            is_new, operation = await self.upsert_venue(venue, site_id)
            
            if operation == "created":
                created += 1
            elif operation == "updated":
                updated += 1
            else:
                errors += 1
        
        result = {
            "created": created,
            "updated": updated,
            "errors": errors,
            "total": len(venues)
        }
        
        logger.info(f"📊 Batch upsert for {site_id}: {result}")
        return result
    
    async def get_all_venues(self, skip: int = 0, limit: int = 1000, site_id: Optional[str] = None) -> List[dict]:
        """Get all venues with optional site filter."""
        try:
            query = {}
            if site_id:
                query["source_site_id"] = site_id
            
            cursor = self.venues_collection.find(query).skip(skip).limit(limit).sort("created_at", -1)
            venues = await cursor.to_list(length=limit)
            
            for venue in venues:
                venue['_id'] = str(venue['_id'])
            
            return venues
        except Exception as e:
            logger.error(f"❌ Error fetching venues: {e}")
            return []
    
    async def get_venue_count(self, site_id: Optional[str] = None) -> int:
        """Get total venue count, optionally filtered by site."""
        try:
            query = {}
            if site_id:
                query["source_site_id"] = site_id
            
            count = await self.venues_collection.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"❌ Error counting venues: {e}")
            return 0
    
    async def search_venues(self, query: str, skip: int = 0, limit: int = 1000, site_id: Optional[str] = None) -> List[dict]:
        """Search venues with optional site filter."""
        try:
            search_filter = {
                "$or": [
                    {"name": {"$regex": query, "$options": "i"}},
                    {"location": {"$regex": query, "$options": "i"}},
                    {"description": {"$regex": query, "$options": "i"}}
                ]
            }
            
            if site_id:
                search_filter["source_site_id"] = site_id
            
            cursor = self.venues_collection.find(search_filter).skip(skip).limit(limit)
            venues = await cursor.to_list(length=limit)
            
            for venue in venues:
                venue['_id'] = str(venue['_id'])
            
            return venues
        except Exception as e:
            logger.error(f"❌ Error searching venues: {e}")
            return []
    
    async def delete_old_venues(self, days: int = 30, site_id: Optional[str] = None) -> int:
        """Delete venues not scraped in the last N days."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = {"last_scraped": {"$lt": cutoff_date}}
            
            if site_id:
                query["source_site_id"] = site_id
            
            result = await self.venues_collection.delete_many(query)
            deleted_count = result.deleted_count
            
            logger.info(f"🗑️ Deleted {deleted_count} old venues from {site_id or 'all sites'}")
            return deleted_count
        except Exception as e:
            logger.error(f"❌ Error deleting old venues: {e}")
            return 0
    
    async def get_venues_by_location(self, location: str, skip: int = 0, limit: int = 100) -> List[dict]:
        """Get venues by location."""
        try:
            cursor = self.venues_collection.find(
                {"location": {"$regex": location, "$options": "i"}}
            ).skip(skip).limit(limit)
            
            venues = await cursor.to_list(length=limit)
            for venue in venues:
                venue['_id'] = str(venue['_id'])
            
            return venues
        except Exception as e:
            logger.error(f"❌ Error fetching venues by location: {e}")
            return []
    
    # ==================== SCRAPING LOG OPERATIONS ====================
    
    async def create_scraping_log(self, site_id: str, site_name: str) -> str:
        """Create a new scraping log entry."""
        try:
            log = {
                "site_id": site_id,
                "site_name": site_name,
                "started_at": datetime.utcnow(),
                "status": "running",
                "venues_found": 0,
                "venues_created": 0,
                "venues_updated": 0,
                "errors": 0
            }
            
            result = await self.logs_collection.insert_one(log)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ Error creating scraping log: {e}")
            return None
    
    async def update_scraping_log(self, log_id: str, update_data: dict):
        """Update scraping log with results."""
        try:
            from bson import ObjectId
            
            update_data['completed_at'] = datetime.utcnow()
            
            # Calculate duration if started_at exists
            log = await self.logs_collection.find_one({"_id": ObjectId(log_id)})
            if log and 'started_at' in log:
                duration = (update_data['completed_at'] - log['started_at']).total_seconds()
                update_data['duration_seconds'] = duration
            
            await self.logs_collection.update_one(
                {"_id": ObjectId(log_id)},
                {"$set": update_data}
            )
        except Exception as e:
            logger.error(f"❌ Error updating scraping log: {e}")
    
    async def get_scraping_logs(self, site_id: Optional[str] = None, limit: int = 50) -> List[dict]:
        """Get scraping logs."""
        try:
            query = {}
            if site_id:
                query["site_id"] = site_id
            
            cursor = self.logs_collection.find(query).sort("started_at", -1).limit(limit)
            logs = await cursor.to_list(length=limit)
            
            for log in logs:
                log['_id'] = str(log['_id'])
            
            return logs
        except Exception as e:
            logger.error(f"❌ Error fetching logs: {e}")
            return []
    
    async def get_site_statistics(self, site_id: str) -> dict:
        """Get statistics for a specific site."""
        try:
            total_venues = await self.get_venue_count(site_id=site_id)
            
            # Get last successful scrape
            last_log = await self.logs_collection.find_one(
                {"site_id": site_id, "status": "success"},
                sort=[("completed_at", -1)]
            )
            
            # Get recent logs
            recent_logs = await self.get_scraping_logs(site_id=site_id, limit=10)
            
            return {
                "site_id": site_id,
                "total_venues": total_venues,
                "last_scrape": last_log.get("completed_at") if last_log else None,
                "last_scrape_result": {
                    "venues_found": last_log.get("venues_found", 0),
                    "venues_created": last_log.get("venues_created", 0),
                    "venues_updated": last_log.get("venues_updated", 0),
                } if last_log else None,
                "recent_scrapes": len(recent_logs),
                "recent_logs": recent_logs[:5]  # Last 5
            }
        except Exception as e:
            logger.error(f"❌ Error getting site statistics: {e}")
            return {}


# ==================== GLOBAL INSTANCE ====================
# This is what was missing!
mongodb_manager = MongoDBManager()