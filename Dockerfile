# Use a slim Python base image for a smaller footprint
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Set a non-root user
RUN useradd --create-home appuser
USER appuser

# Set an environment variable for unbuffered python output
ENV PYTHONUNBUFFERED=1

# Install uv, a fast Python package installer
RUN pip install --user uv

# Copy only the dependency files first to leverage Docker's layer caching
COPY --chown=appuser:appuser pyproject.toml requirements.txt ./

# Install dependencies using uv
RUN /home/appuser/.local/bin/uv pip install --system --no-cache -r requirements.txt
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV TEMP_DIR=/tmp/video_processing
ENV FFMPEG_PATH=/usr/bin/ffmpeg
ENV FFPROBE_PATH=/usr/bin/ffprobe
# Copy the rest of the application code
COPY --chown=appuser:appuser . .

# Command to run the application
CMD ["/home/appuser/.local/bin/uv", "run", "python", "src/main.py"]