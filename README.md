# Discord Conversational Intelligent Platform

A real-time Discord message ingestion service that stores chat data in MongoDB Atlas. Part of an intelligent platform for conversational analysis.

## Features
- Real-time message capture using Discord.py.
- Asynchronous storage in MongoDB Atlas via Motor.
- Historical message fetching support.
- Production-ready (Render/Docker support).

## Setup
1. Clone the repository.
2. Create a `.env` file from `.env.example`.
3. Install dependencies: `pip install -r requirements.txt`.
4. Run: `python run_bot.py`.
