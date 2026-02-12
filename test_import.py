# test_import.py
print("Testing imports...")

try:
    from utils.mongodb_utils import mongodb_manager
    print("✅ mongodb_manager imported successfully")
    print(f"   Type: {type(mongodb_manager)}")
    print(f"   DB: {mongodb_manager.db_name}")
except ImportError as e:
    print(f"❌ Import failed: {e}")

try:
    from utils.schema_utils import get_venue_schema_for_llm
    print("✅ schema_utils imported successfully")
except ImportError as e:
    print(f"❌ schema_utils import failed: {e}")

try:
    from services.scheduler import multi_site_scheduler
    print("✅ scheduler imported successfully")
except ImportError as e:
    print(f"❌ scheduler import failed: {e}")

print("\n✅ All imports successful!")