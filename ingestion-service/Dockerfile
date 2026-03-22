# Use an official lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies if needed (none for this basic bot)
# RUN apt-get update && apt-get install -y --no-install-recommends ...

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# The bot doesn't need to expose any ports (it's a pull-based client)
# If you add a web dashboard later, you'd EXPOSE 8080 here.

# Entry point
CMD ["python", "run_bot.py"]
