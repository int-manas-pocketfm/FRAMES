# FRAMES — Shot Generation Pipeline (Python 3.12 / Flask)

# ── Stage 1: Install Python dependencies ──
FROM python:3.12-slim AS deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 2: Production runtime ──
FROM python:3.12-slim AS runner
WORKDIR /app

# System dependencies: FFmpeg (Stage 6 video), libgomp (faster-whisper)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from deps stage
COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

# Non-root user
RUN groupadd --system --gid 1001 appgroup && \
    useradd --system --uid 1001 --gid appgroup appuser

# Copy application files
COPY --chown=appuser:appgroup server.py .
COPY --chown=appuser:appgroup templates/ ./templates/
COPY --chown=appuser:appgroup static/ ./static/
COPY --chown=appuser:appgroup prompts/ ./prompts/

# Workspace directories for job output (images, Excel, video)
RUN mkdir -p /app/workspace && chown -R appuser:appgroup /app/workspace

ENV PORT=5000
ENV PYTHONUNBUFFERED=1

USER appuser
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:5000/ || exit 1

CMD ["python", "server.py"]
