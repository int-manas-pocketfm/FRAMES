# FRAMES — Shot Generation Pipeline

A 6-stage pipeline that transforms episode scripts into production-ready 1080x1080 videos with AI-generated images and subtitles.

| Stage | What it does |
|-------|-------------|
| 1 | Generate reference files (tone bible, character canvas, location reference) from scripts |
| 2 | Break scripts into shot-by-shot Excel breakdowns with AI image-gen-ready descriptions |
| 3 | Audit shot breakdowns against source scripts and auto-fix coverage gaps |
| 4 | Generate character and location reference images |
| 5 | Generate per-shot images anchored to character/location references |
| 6 | Composite images + audio into final MP4 videos with optional subtitles |

## Prerequisites

- **Python 3.10+**
- **FFmpeg** — required for Stage 6 video generation. [Download](https://ffmpeg.org/download.html) and add to PATH.

## Setup

1. Clone the repo:
   ```
   git clone https://github.com/GarvAgarwal1404/Shot-Generation.git
   cd Shot-Generation
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create your `.env` file from the example:
   ```
   cp .env.example .env
   ```

4. Fill in your API keys in `.env`:
   - **ARGUS_API_KEY / ARGUS_BASE_URL / ARGUS_MODEL** — Claude API access (Stages 1-3)
   - **MADEYE_API_KEY / MADEYE_BASE_URL** — Gemini Imagen 4 access (Stages 4-5)
   - **SUPABASE_URL / SUPABASE_ANON_KEY** — Database for storing shows and reference files
   - **GOOGLE_SERVICE_ACCOUNT_JSON** — (Optional) For Stage 5 Google Sheets export

## Run

```
python server.py
```

Open **http://localhost:5000** in your browser.

## Project Structure

```
server.py              — Backend: all routes, pipelines, API calls, video rendering
templates/index.html   — Web UI: dark-themed dashboard with 6 stage tabs
prompts/               — LLM prompt templates for Stage 1 outputs
static/                — Logo and static assets
workspace/             — Runtime directory (auto-created, stores job outputs)
.env.example           — Environment variable template
```
