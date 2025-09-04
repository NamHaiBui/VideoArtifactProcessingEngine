# Use a slim Python base image for a smaller footprint
FROM python:3.11-slim

# Install system dependencies (including ffmpeg) before switching to non-root user
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Set a non-root user
RUN useradd --create-home appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV TEMP_DIR=/tmp/video_processing
ENV FFMPEG_PATH=/usr/bin/ffmpeg
ENV FFPROBE_PATH=/usr/bin/ffprobe

# Install uv as root before switching users
RUN pip install uv

# Copy only the dependency files first to leverage Docker's layer caching
COPY --chown=appuser:appuser pyproject.toml requirements.txt ./

# Install dependencies using uv (as root before switching users)
RUN uv pip install --system --no-cache -r requirements.txt

# Ensure /app is owned by appuser before switching users
RUN chown -R appuser:appuser /app

# Switch to non-root user after installing dependencies
USER appuser

# Copy the rest of the application code
COPY --chown=appuser:appuser . .

# Command to run the application
CMD ["uv", "run", "python", "src/main.py"]