# Discord Conversational Intelligent Platform

This is a monorepo containing multiple microservices for the intelligent platform.

## Project Structure
- `ingestion-service/`: Discord bot that captures real-time messages and stores them in MongoDB Atlas.
- (More services coming soon...)

## Ingestion Service Setup
1. Clone the repository.
2. `cd ingestion-service`
3. Create a `.env` file from `.env.example`.
4. Install dependencies: `pip install -r requirements.txt`.
5. Run: `python run_bot.py`.
