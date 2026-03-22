"""
tests/test_db.py
----------------
Unit tests for the db layer (connection, schemas, crud).
Uses mongomock-motor so no real MongoDB instance is needed.

Run with:
    pytest tests/test_db.py -v
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from mongomock_motor import AsyncMongoMockClient

from db.schemas import AttachmentDoc, MessageDoc
from db.crud import (
    upsert_message,
    upsert_messages_bulk,
    get_message,
    get_messages_by_channel,
    get_unembedded_messages,
    mark_embedded,
    get_stats,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
def mock_db():
    """Return an in-memory Motor-compatible database."""
    client = AsyncMongoMockClient()
    return client["test_discord_intel"]


def _make_msg(
    message_id: str = "111",
    channel_id: str = "999",
    content: str = "Hello world",
    has_embedding: bool = False,
) -> MessageDoc:
    return MessageDoc(
        message_id=message_id,
        guild_id="777",
        channel_id=channel_id,
        channel_name="general",
        author_id="123",
        author_name="testuser",
        is_bot=False,
        content=content,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        has_embedding=has_embedding,
    )


# ─── upsert_message ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upsert_message_inserts_new(mock_db):
    msg = _make_msg(message_id="AAA")
    inserted = await upsert_message(msg, db=mock_db)
    assert inserted is True


@pytest.mark.asyncio
async def test_upsert_message_idempotent(mock_db):
    """Upserting the same message_id twice should not insert a duplicate."""
    msg = _make_msg(message_id="BBB")
    await upsert_message(msg, db=mock_db)
    inserted_again = await upsert_message(msg, db=mock_db)
    assert inserted_again is False

    count = await mock_db["messages"].count_documents({"message_id": "BBB"})
    assert count == 1


# ─── upsert_messages_bulk ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_bulk_upsert_returns_correct_counts(mock_db):
    msgs = [_make_msg(str(i)) for i in range(5)]
    inserted, skipped = await upsert_messages_bulk(msgs, db=mock_db)
    assert inserted == 5
    assert skipped == 0


@pytest.mark.asyncio
async def test_bulk_upsert_empty_list(mock_db):
    inserted, skipped = await upsert_messages_bulk([], db=mock_db)
    assert inserted == 0
    assert skipped == 0


@pytest.mark.asyncio
async def test_bulk_upsert_skips_duplicates(mock_db):
    msgs = [_make_msg(str(i)) for i in range(3)]
    await upsert_messages_bulk(msgs, db=mock_db)
    # Re-insert same 3 + 2 new
    more = msgs + [_make_msg("100"), _make_msg("101")]
    inserted, skipped = await upsert_messages_bulk(more, db=mock_db)
    assert inserted == 2
    assert skipped == 3


# ─── get_message ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_message_found(mock_db):
    msg = _make_msg(message_id="CCC", content="Find me")
    await upsert_message(msg, db=mock_db)
    doc = await get_message("CCC", db=mock_db)
    assert doc is not None
    assert doc["content"] == "Find me"


@pytest.mark.asyncio
async def test_get_message_not_found(mock_db):
    doc = await get_message("NONEXISTENT", db=mock_db)
    assert doc is None


# ─── get_messages_by_channel ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_messages_by_channel(mock_db):
    msgs = [_make_msg(str(i), channel_id="CH1") for i in range(10, 15)]
    await upsert_messages_bulk(msgs, db=mock_db)

    results = await get_messages_by_channel("CH1", limit=10, db=mock_db)
    assert len(results) == 5
    assert all(r["channel_id"] == "CH1" for r in results)


# ─── get_unembedded_messages ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_unembedded_only_returns_unembedded(mock_db):
    unembedded = [_make_msg(str(i), has_embedding=False) for i in range(20, 25)]
    embedded = [_make_msg(str(i), has_embedding=True) for i in range(25, 30)]
    await upsert_messages_bulk(unembedded + embedded, db=mock_db)

    results = await get_unembedded_messages(batch_size=50, db=mock_db)
    assert len(results) == 5
    assert all(r["has_embedding"] is False for r in results)


# ─── mark_embedded ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_mark_embedded_flips_flag(mock_db):
    msgs = [_make_msg(str(i)) for i in range(30, 35)]
    await upsert_messages_bulk(msgs, db=mock_db)

    ids = [str(i) for i in range(30, 35)]
    modified = await mark_embedded(ids, db=mock_db)
    assert modified == 5

    pending = await get_unembedded_messages(batch_size=50, db=mock_db)
    pending_ids = {p["message_id"] for p in pending}
    assert not any(mid in pending_ids for mid in ids)


@pytest.mark.asyncio
async def test_mark_embedded_empty_list(mock_db):
    result = await mark_embedded([], db=mock_db)
    assert result == 0


# ─── get_stats ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_stats(mock_db):
    msgs = [_make_msg(str(i), has_embedding=(i % 2 == 0)) for i in range(40, 50)]
    await upsert_messages_bulk(msgs, db=mock_db)

    stats = await get_stats(db=mock_db)
    assert stats["total_messages"] == 10
    assert stats["embedded"] == 5
    assert stats["pending_embedding"] == 5


# ─── Schema ───────────────────────────────────────────────────────────────────

def test_message_doc_serialises():
    msg = _make_msg()
    d = msg.to_mongo()
    assert d["message_id"] == "111"
    assert d["has_embedding"] is False
    assert isinstance(d["timestamp"], datetime)


def test_attachment_doc():
    a = AttachmentDoc(
        id="99",
        filename="photo.png",
        url="https://cdn.discordapp.com/attachments/99/photo.png",
        content_type="image/png",
        size=204800,
    )
    assert a.size == 204800
