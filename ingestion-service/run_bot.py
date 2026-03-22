"""
run_bot.py
----------
Entry point for the real-time Discord message listener.

    python run_bot.py

The bot will:
  1. Ensure MongoDB indexes exist.
  2. Connect to Discord.
  3. Capture every new non-bot message and upsert it to MongoDB.

Press Ctrl+C to stop.
"""

from bot.bot import main

if __name__ == "__main__":
    main()
