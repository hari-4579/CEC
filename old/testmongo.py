import sys
import socket
from pymongo import MongoClient, errors


MONGO_URI = "mongodb://mongo:27017"

print(f"Connecting to MongoDB at {MONGO_URI} ...")

try:
    # Create client and timeout for server selection 
    conn = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    # Force an immediate connection check
    conn.admin.command("ping")

    # Print some useful info
    info = conn.server_info()
    print(" Connected to MongoDB!")
    print(f"    MongoDB version: {info.get('version')}")
    print(f"    Client hostname: {socket.gethostname()}")

except errors.ServerSelectionTimeoutError as e:
    print(" Could not connect to MongoDB:", e)
    sys.exit(1)
except Exception as e:
    print(" Unexpected error while connecting to MongoDB:", e)
    sys.exit(1)

# database setup
db = conn["test"]
experiments = db["experiments"]
experiments_records = db["records"]
print(" Database and collections are ready.")