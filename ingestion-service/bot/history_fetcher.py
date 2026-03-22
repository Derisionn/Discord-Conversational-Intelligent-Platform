"""
bot/history_fetcher.py
-----------------------
One-shot async script that fetches ALL historical messages from every
text channel in the configured guild and bulk-upserts them into MongoDB.

Features
--------
- Skips channels the bot can't read (no permissions)
- Paginates automatically — no memory cap (uses async generator)
- Bulk insert in configurable batches for throughput
- Rich progress display (channel-by-channel, running totals)
- Safe to re-run: duplicate messages are silently skipped (upsert)

Usage
-----
    python bot/history_fetcher.py
    python run_history.py         # entry-point wrapper
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timezone

import discord
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from db.connection import ensure_indexes
from db.crud import upsert_messages_bulk, get_stats
from db.schemas import AttachmentDoc, MessageDoc

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
DISCORD_TOKEN: str = os.getenv("DISCORD_TOKEN", "")
DISCORD_GUILD_ID: int = int(os.getenv("DISCORD_GUILD_ID", "0"))
HISTORY_LIMIT: int | None = int(os.getenv("HISTORY_LIMIT", "5000")) or None
BATCH_SIZE: int = 200   # how many messages to bulk-insert at once

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)
log = logging.getLogger("history_fetcher")
console = Console()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _to_doc(message: discord.Message, guild_id: str) -> MessageDoc:
    """Convert a discord.Message to a MessageDoc."""
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

    # Ensure timezone-aware datetime
    ts = message.created_at
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return MessageDoc(
        message_id=str(message.id),
        guild_id=guild_id,
        channel_id=str(message.channel.id),
        channel_name=message.channel.name,          # type: ignore[attr-defined]
        author_id=str(message.author.id),
        author_name=str(message.author),
        is_bot=message.author.bot,
        content=message.content,
        timestamp=ts,
        edited_at=message.edited_at,
        attachments=attachments,
        embeds_count=len(message.embeds),
        reply_to_id=reply_to_id,
        has_embedding=False,
    )


async def fetch_channel_history(
    channel: discord.TextChannel,
    guild_id: str,
    progress: Progress,
    task_id,
) -> tuple[int, int]:
    """
    Fetch all messages from `channel` and bulk-upsert them.

    Returns
    -------
    (total_fetched, total_inserted)
    """
    fetched = 0
    inserted_total = 0
    batch: list[MessageDoc] = []

    try:
        async for message in channel.history(limit=HISTORY_LIMIT, oldest_first=True):
            batch.append(_to_doc(message, guild_id))
            fetched += 1

            if len(batch) >= BATCH_SIZE:
                ins, _ = await upsert_messages_bulk(batch)
                inserted_total += ins
                batch.clear()
                progress.update(task_id, advance=BATCH_SIZE)

        # Flush remaining
        if batch:
            ins, _ = await upsert_messages_bulk(batch)
            inserted_total += ins
            progress.update(task_id, advance=len(batch))
            batch.clear()

    except discord.Forbidden:
        log.warning("No read permission for #%s — skipping.", channel.name)
    except discord.HTTPException as exc:
        log.error("HTTP error fetching #%s: %s", channel.name, exc)

    return fetched, inserted_total


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run_fetcher() -> None:
    """Main coroutine — initialises Discord client and fetches all history."""

    if not DISCORD_TOKEN:
        raise SystemExit(
            "[ERROR] DISCORD_TOKEN not set. "
            "Copy .env.example → .env and fill in your bot token."
        )
    if not DISCORD_GUILD_ID:
        raise SystemExit(
            "[ERROR] DISCORD_GUILD_ID not set. "
            "Right-click your server → Copy Server ID."
        )

    await ensure_indexes()

    intents = discord.Intents.default()
    intents.message_content = True
    intents.guilds = True

    client = discord.Client(intents=intents)
    ready_event = asyncio.Event()
    results: list[tuple[str, int, int]] = []   # (channel_name, fetched, inserted)

    @client.event
    async def on_ready() -> None:
        ready_event.set()

    async with client:
        # Start login in the background
        login_task = asyncio.create_task(client.start(DISCORD_TOKEN))

        # Wait until the client is connected
        await ready_event.wait()

        guild = client.get_guild(DISCORD_GUILD_ID)
        if guild is None:
            log.error(
                "Guild %s not found. Make sure the bot is invited to the server.",
                DISCORD_GUILD_ID,
            )
            await client.close()
            await login_task
            return

        text_channels = [
            ch for ch in guild.channels
            if isinstance(ch, discord.TextChannel)
        ]

        console.rule(f"[bold cyan]History Fetcher[/bold cyan]")
        console.print(
            f"  Guild    : [bold]{guild.name}[/bold]\n"
            f"  Channels : {len(text_channels)} text channels\n"
            f"  Limit    : {'ALL' if HISTORY_LIMIT is None else HISTORY_LIMIT} msgs/channel\n"
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            for channel in text_channels:
                task_id = progress.add_task(
                    f"#{channel.name}",
                    total=HISTORY_LIMIT or 999_999,
                )
                fetched, inserted = await fetch_channel_history(
                    channel, str(guild.id), progress, task_id
                )
                results.append((channel.name, fetched, inserted))
                progress.update(task_id, completed=fetched, total=fetched)

        await client.close()
        await login_task

    # ── Summary table ─────────────────────────────────────────────────────────
    table = Table(title="History Fetch Summary", show_lines=True)
    table.add_column("Channel", style="cyan")
    table.add_column("Fetched", justify="right")
    table.add_column("Newly Inserted", justify="right", style="green")
    table.add_column("Skipped (dup)", justify="right", style="dim")

    for ch_name, fetched, inserted in results:
        table.add_row(
            f"#{ch_name}",
            str(fetched),
            str(inserted),
            str(fetched - inserted),
        )

    console.print(table)

    stats = await get_stats()
    console.print(
        f"\n[bold]MongoDB totals:[/bold]  "
        f"[green]{stats['total_messages']}[/green] messages stored  |  "
        f"[yellow]{stats['pending_embedding']}[/yellow] pending embedding"
    )


if __name__ == "__main__":
    asyncio.run(run_fetcher())
