"""
bot/bot.py
----------
Real-time Discord bot that listens for new messages and persists them
to MongoDB via the db.crud layer.

Usage
-----
    python run_bot.py          # preferred (uses the entry point)
    python -m bot.bot          # run directly

Required env vars (set in .env):
    DISCORD_TOKEN
    DISCORD_GUILD_ID
"""

from __future__ import annotations

import asyncio
import logging
import os

import discord
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler

from db.connection import ensure_indexes
from db.crud import upsert_message
from db.schemas import AttachmentDoc, MessageDoc

load_dotenv()

# ─── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)
log = logging.getLogger("discord_bot")
console = Console()

# ─── Discord intents ──────────────────────────────────────────────────────────
# message_content intent must be enabled in the Developer Portal:
#   https://discord.com/developers/applications → Bot → Privileged Gateway Intents
intents = discord.Intents.default()
intents.message_content = True   # required to read message text
intents.messages = True
intents.guilds = True
intents.members = False           # not needed for message capture


# ─── Helper ───────────────────────────────────────────────────────────────────

def _build_message_doc(message: discord.Message) -> MessageDoc:
    """Convert a discord.Message into a MessageDoc ready for MongoDB."""
    attachments = [
        AttachmentDoc(
            id=str(a.id),
            filename=a.filename,
            url=a.url,
            content_type=a.content_type,
            size=a.size,
        )
        for a in message.attachments
    ]

    reply_to_id: str | None = None
    if message.reference and message.reference.message_id:
        reply_to_id = str(message.reference.message_id)

    return MessageDoc(
        message_id=str(message.id),
        guild_id=str(message.guild.id) if message.guild else "DM",
        channel_id=str(message.channel.id),
        channel_name=getattr(message.channel, "name", "DM"),
        author_id=str(message.author.id),
        author_name=str(message.author),          # e.g. "username#1234" or "username"
        is_bot=message.author.bot,
        content=message.content,
        timestamp=message.created_at,
        edited_at=message.edited_at,
        attachments=attachments,
        embeds_count=len(message.embeds),
        reply_to_id=reply_to_id,
        has_embedding=False,
    )


# ─── Client ───────────────────────────────────────────────────────────────────

class DiscordIntelBot(discord.Client):
    """
    A discord.Client subclass that:
    - Ensures MongoDB indexes on startup.
    - Captures every non-bot message and upserts it to MongoDB.
    """

    def __init__(self, **kwargs):
        super().__init__(intents=intents, **kwargs)
        self._guild_id: int = int(os.getenv("DISCORD_GUILD_ID", "0"))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup_hook(self) -> None:
        """Called once before the client is ready.  Good place for async init."""
        await ensure_indexes()
        log.info("MongoDB indexes ensured.")

    async def on_ready(self) -> None:
        guild = discord.utils.get(self.guilds, id=self._guild_id)
        guild_name = guild.name if guild else "unknown guild"
        console.rule(f"[bold green]Bot online[/bold green]")
        console.print(
            f"  Logged in as : [bold]{self.user}[/bold]\n"
            f"  Guild        : [cyan]{guild_name}[/cyan] (id={self._guild_id})\n"
            f"  Listening for new messages…",
        )

    async def on_disconnect(self) -> None:
        log.warning("Disconnected from Discord. Will attempt to reconnect…")

    # ── Message capture ───────────────────────────────────────────────────────

    async def on_message(self, message: discord.Message) -> None:
        """Fired for every new message the bot can see."""

        # Ignore messages from bots (including ourselves)
        if message.author.bot:
            return

        # Restrict to the configured guild (ignore DMs / other servers)
        if message.guild is None or message.guild.id != self._guild_id:
            return

        doc = _build_message_doc(message)

        try:
            inserted = await upsert_message(doc)
            status = "[green]NEW[/green]" if inserted else "[dim]DUP[/dim]"
            console.print(
                f"  {status} #{message.channel.name} "   # type: ignore[attr-defined]
                f"[dim]{message.author}[/dim]: "
                f"{message.content[:80]!r}",
                highlight=False,
            )
        except Exception as exc:
            log.error("Failed to persist message %s: %s", message.id, exc)

    # ── Error handling ────────────────────────────────────────────────────────

    async def on_error(self, event_method: str, *args, **kwargs) -> None:
        log.exception("Unhandled error in event '%s'", event_method)


import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    log.info(f"Health check server running on port {port}")
    server.serve_forever()

# ─── Entry point (when run directly) ─────────────────────────────────────────

def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit(
            "[ERROR] DISCORD_TOKEN not set. "
            "Copy .env.example → .env and fill in your bot token."
        )
    
    # Start health check server in a background thread for Render
    threading.Thread(target=run_health_server, daemon=True).start()

    client = DiscordIntelBot()
    client.run(token, log_handler=None)   # we handle logging ourselves


if __name__ == "__main__":
    main()
