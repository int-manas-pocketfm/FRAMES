# FRAMES — Shot Generation Pipeline

## What this is
A Flask web app that transforms episode scripts into production-ready 1080x1080 videos with AI-generated images and subtitles. 6-stage pipeline + automated Pipeline mode.

## Tech stack
- **Backend:** Python 3.12, Flask, single file `server.py` (~5500 lines)
- **Frontend:** Single-page app in `templates/index.html` (~4500 lines), dark/light theme
- **APIs:** Argus (Claude) for LLM stages, MadEye (Gemini Imagen 4) for image gen, Supabase for DB
- **Local tools:** FFmpeg (video), faster-whisper (audio alignment)

## How to run
```
pip install -r requirements.txt
cp .env.example .env   # fill in API keys
python server.py       # opens on localhost:5000
```

## Project structure
```
server.py              — All backend logic, API routes, 6 stage pipelines, pipeline orchestrator
templates/index.html   — Complete frontend: home screen, stage views, pipeline dashboard
prompts/               — LLM prompt templates for Stage 1
static/                — Logo and static assets
.env                   — API keys (not committed)
```

## The 6 stages
1. **Reference Files** — Generate tone bible, character canvas, location reference from scripts (Argus/Claude)
2. **Shot Breakdown** — Script to shot-by-shot Excel with beat coverage (Argus/Claude). Uses two-pass beat extraction + chunked generation for long scripts.
3. **Script Audit** — Verify breakdown Lines match script verbatim. Flags: paraphrased (A), invented (B), truncated (C), duplicate intermediates (D). Key rules: verbatim-only comparison, no contextual reinterpretation, no cascade fixes, split-aware.
4. **Reference Images** — Character portraits + location shots (MadEye/Gemini Imagen 4, $0.02/img)
5. **Shot Images** — Per-shot images anchored to character/location refs (MadEye, $0.02/img)
6. **Video Generation** — Composite images + audio into MP4 with Whisper-aligned subtitles

## Pipeline mode
Chains Stage 1 → 2 → 3+4 (parallel) → 5 automatically. Pauses after Stage 4 for reference image review. Shows per-episode shot images as they complete during Stage 5.

## Key architecture patterns
- Each stage has `_run_stageN_core()` (reusable logic) + `run_stageN_pipeline()` (standalone wrapper)
- Jobs tracked in global `jobs` dict with thread-safe token tracking
- SSE streaming for live logs, 3s polling for status
- `MadEyeKeyPool` for multi-key parallel image generation
- `_split_script_into_chunks()` for Stage 2 token optimization
- Coverage functions: `_measure_coverage`, `_find_missing_sentences`, `_find_rephrased`, `_find_truncated_lines`

## Stage 3 audit rules (important — don't change these)
- **Verbatim only:** If a Line matches the script text character-for-character, it is CORRECT. Never fix based on context.
- **No cascade fixes:** Never shift Lines from one shot to the next in a chain.
- **Split-aware Problem C:** Consecutive complete sentences on separate shots are NOT truncation. Only flag when a single sentence is cut mid-clause.
- **Split-aware Problem D:** Intermediate shots (5.1, 5.2) must carry their OWN portion of text, not duplicate the parent's Line.

## Pricing
- LLM (Sonnet): $3/$15 per MTok input/output
- LLM (Opus): $5/$25 per MTok
- Images: $0.02/image (Stages 4+5)
- Stage 6: Free (local FFmpeg + Whisper)

## Environment variables
```
ARGUS_API_KEY, ARGUS_BASE_URL, ARGUS_MODEL     — Claude API
MADEYE_API_KEY, MADEYE_BASE_URL                — Gemini Imagen
MADEYE_API_KEYS                                — Optional comma-separated for parallel gen
SUPABASE_URL, SUPABASE_ANON_KEY                — Database
GOOGLE_SERVICE_ACCOUNT_JSON                    — Optional, for Google Sheets export
```

## Common tasks
- **Run individual stage:** Click stage tab, upload inputs, click Run
- **Run full pipeline:** Home → Pipeline → upload scripts → Run Pipeline
- **Audit a breakdown:** Stage 3, upload Excel + source script
- **Regenerate images:** Stage 4/5, Review tab, give feedback, click Regenerate

## What NOT to change
- The `PIPELINE_INSTRUCTIONS` and `RULEBOOK` prompts in server.py are carefully tuned — modify with caution
- The Stage 3 `S3_AUDIT_SYSTEM_PROMPT` has specific rules about verbatim comparison and split-awareness that prevent false positives
- The `Line` column in shot breakdowns is always verbatim script text — never paraphrase
