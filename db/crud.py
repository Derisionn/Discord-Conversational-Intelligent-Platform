"""
db/crud.py
----------
Async CRUD helpers for the `messages` collection.
All functions accept an optional `db` parameter so they can be used with
a real Motor database or a mongomock-motor database in tests.
"""

from __future__ import annotations

from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from db.connection import get_db
from db.schemas import MessageDoc


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_message(
    msg: MessageDoc,
    db: AsyncIOMotorDatabase | None = None,
) -> bool:
    """
    Insert `msg` if its message_id doesn't exist yet; skip if it does.

    Returns
    -------
    True  — document was newly inserted
    False — document already existed (no-op)
    """
    db = db or get_db()
    result = await db["messages"].update_one(
        {"message_id": msg.message_id},
        {"$setOnInsert": msg.to_mongo()},
        upsert=True,
    )
    return result.upserted_id is not None


async def upsert_messages_bulk(
    msgs: list[MessageDoc],
    db: AsyncIOMotorDatabase | None = None,
) -> tuple[int, int]:
    """
    Bulk-upsert a list of messages.

    Returns
    -------
    (inserted_count, skipped_count)
    """
    if not msgs:
        return 0, 0

    db = db or get_db()
    operations = [
        UpdateOne(
            {"message_id": m.message_id},
            {"$setOnInsert": m.to_mongo()},
            upsert=True,
        )
        for m in msgs
    ]

    try:
        result = await db["messages"].bulk_write(operations, ordered=False)
        inserted = result.upserted_count
        skipped = len(msgs) - inserted
        return inserted, skipped
    except BulkWriteError as bwe:
        # Partial success — some may have been inserted
        inserted = bwe.details.get("nUpserted", 0)
        skipped = len(msgs) - inserted
        return inserted, skipped


# ─────────────────────────────────────────────────────────────────────────────
# Read helpers
# ─────────────────────────────────────────────────────────────────────────────

async def get_message(
    message_id: str,
    db: AsyncIOMotorDatabase | None = None,
) -> dict[str, Any] | None:
    """Return a single message document by its Discord message_id."""
    db = db or get_db()
    return await db["messages"].find_one({"message_id": message_id})


async def get_messages_by_channel(
    channel_id: str,
    limit: int = 100,
    db: AsyncIOMotorDatabase | None = None,
) -> list[dict[str, Any]]:
    """Return the latest `limit` messages for a given channel, newest first."""
    db = db or get_db()
    cursor = (
        db["messages"]
        .find({"channel_id": channel_id})
        .sort("timestamp", -1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def get_unembedded_messages(
    batch_size: int = 100,
    db: AsyncIOMotorDatabase | None = None,
) -> list[dict[str, Any]]:
    """
    Return up to `batch_size` messages that have not yet been embedded.
    Used by the embedding pipeline (implemented later).
    """
    db = db or get_db()
    cursor = (
        db["messages"]
        .find({"has_embedding": False, "content": {"$ne": ""}})
        .limit(batch_size)
    )
    return await cursor.to_list(length=batch_size)


async def mark_embedded(
    message_ids: list[str],
    db: AsyncIOMotorDatabase | None = None,
) -> int:
    """
    Set has_embedding=True for all given message_ids.
    Returns the number of documents modified.
    """
    if not message_ids:
        return 0
    db = db or get_db()
    result = await db["messages"].update_many(
        {"message_id": {"$in": message_ids}},
        {"$set": {"has_embedding": True}},
    )
    return result.modified_count


# ─────────────────────────────────────────────────────────────────────────────
# Stats helper
# ─────────────────────────────────────────────────────────────────────────────

async def get_stats(db: AsyncIOMotorDatabase | None = None) -> dict[str, int]:
    """Return high-level counts for quick health-checks."""
    db = db or get_db()
    total = await db["messages"].count_documents({})
    embedded = await db["messages"].count_documents({"has_embedding": True})
    return {
        "total_messages": total,
        "embedded": embedded,
        "pending_embedding": total - embedded,
    }
