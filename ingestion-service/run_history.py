"""
run_history.py
--------------
Entry point for the one-shot historical message fetcher.

    python run_history.py

This script fetches all historical messages from every text channel in
your Discord guild and stores them in MongoDB.  Safe to re-run: duplicate
messages are silently skipped.

Configure HISTORY_LIMIT in your .env to cap messages per channel.
"""

import asyncio
from bot.history_fetcher import run_fetcher

if __name__ == "__main__":
    asyncio.run(run_fetcher())
