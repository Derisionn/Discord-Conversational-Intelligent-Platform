"""
db/connection.py
----------------
Provides a singleton AsyncIOMotorClient and a helper to get the
configured database.  Import `get_db` wherever you need MongoDB access.
"""

from __future__ import annotations

import os
from functools import lru_cache

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "discord_intel")


@lru_cache(maxsize=1)
def _get_client() -> AsyncIOMotorClient:
    """Return a cached Motor client (created once per process)."""
    return AsyncIOMotorClient(MONGODB_URI)


def get_db() -> AsyncIOMotorDatabase:
    """Return the configured database from the cached client."""
    return _get_client()[MONGODB_DB_NAME]


async def ensure_indexes() -> None:
    """
    Create indexes the first time the app starts.
    Safe to call repeatedly — MongoDB is idempotent for index creation.
    """
    db = get_db()
    messages = db["messages"]

    await messages.create_index("message_id", unique=True)
    await messages.create_index("guild_id")
    await messages.create_index("channel_id")
    await messages.create_index("author_id")
    await messages.create_index([("timestamp", -1)])          # latest first
    await messages.create_index("has_embedding")              # for pipeline queries
