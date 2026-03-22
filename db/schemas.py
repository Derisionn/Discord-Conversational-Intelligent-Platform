"""
db/schemas.py
-------------
Pydantic v2 models that represent the shape of documents in MongoDB.
These are NOT ODM models — they are used purely for validation /
serialisation before inserting with Motor.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AttachmentDoc(BaseModel):
    """Represents a file or media attachment on a message."""
    id: str
    filename: str
    url: str
    content_type: str | None = None
    size: int | None = None                   # bytes


class MessageDoc(BaseModel):
    """
    Core document stored in the `messages` collection.

    message_id  — Discord's snowflake ID (string for safe JSON round-trip)
    guild_id    — Server (guild) snowflake
    channel_id  — Channel snowflake
    channel_name— Human-readable channel name at time of capture
    author_id   — Snowflake of the message author
    author_name — Display name (username#discriminator or global name)
    is_bot      — True if the author is a bot
    content     — Raw text of the message (may be empty for embed-only msgs)
    timestamp   — UTC datetime of the original message
    edited_at   — UTC datetime if the message was edited, else None
    attachments — List of attached files / images
    embeds_count— Number of rich embeds (content stored separately if needed)
    reply_to_id — message_id of the parent message if this is a reply
    has_embedding — Flag set by the embedding pipeline once vectorised
    """

    message_id: str
    guild_id: str
    channel_id: str
    channel_name: str
    author_id: str
    author_name: str
    is_bot: bool = False
    content: str = ""
    timestamp: datetime
    edited_at: datetime | None = None
    attachments: list[AttachmentDoc] = Field(default_factory=list)
    embeds_count: int = 0
    reply_to_id: str | None = None
    has_embedding: bool = False

    def to_mongo(self) -> dict[str, Any]:
        """
        Serialise to a plain dict suitable for Motor / pymongo.
        Uses `message_id` as the logical unique key (not _id).
        """
        return self.model_dump()
