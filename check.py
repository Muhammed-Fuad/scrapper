import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

load_dotenv()

async def test_mongo():
    try:
        client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        await db.command("ping")
        print("✅ MongoDB connected successfully!")
    except Exception as e:
        print("❌ Connection failed:", e)

asyncio.run(test_mongo())
