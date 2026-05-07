FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (layer caching)
COPY ingestion-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the service code
COPY ingestion-service/ .

# Start the bot
CMD ["python", "run_bot.py"]
