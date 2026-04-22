#!/usr/bin/env python3
"""
Shot Generation Pipeline — Stage 1 + Stage 2
Single-file backend + web UI.

Run:  python server.py
Open: http://localhost:5000
"""

import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from flask import Flask, Response, jsonify, render_template, request, send_file

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _load_dotenv():
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val

_load_dotenv()


# ── Runtime constants ─────────────────────────────────────────────────────────

ROOT             = Path(__file__).parent
WORKSPACE        = ROOT / "workspace"
WORKSPACE.mkdir(exist_ok=True)

ARGUS_API_KEY    = os.environ.get("ARGUS_API_KEY", "")
ARGUS_BASE_URL   = os.environ.get("ARGUS_BASE_URL", "")
ARGUS_MODEL      = os.environ.get("ARGUS_MODEL", "")

SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY", "")

RATE_LIMIT_WAIT  = 60
BATCH_SIZE       = 20
INTER_CALL_PAUSE = 10
STAGGER_SEC      = 5

SHOW_LEVEL_STEMS = {"show_tone_bible", "character_canvas", "location_reference"}
SHOW_LEVEL_FILES = {s + ".md" for s in SHOW_LEVEL_STEMS}


# ── Embedded prompts — Stage 1 ────────────────────────────────────────────────

PROMPTS = {

"show_tone_bible": """\
OUTPUT CONSTRAINT: Produce a complete but concise document. Avoid padding. Under 1500 words.

---

You are a professional visual development coordinator for an AI-assisted video content pipeline. Your job is to read all available episode scripts for a show and produce a comprehensive Show Tone Bible in markdown format.

This file is a permanent show-level reference used across every episode's image generation process. It must capture the complete visual language of the show — color, light, mood, motif — consistently enough that any episode's shots can be generated in a unified style.

---

## OUTPUT FORMAT

---

# Show Tone Bible — [Show Name]

## Show Overview
3–5 sentences. Genre, setting, core emotional register, and the visual world the show inhabits. Written for a cinematographer or visual effects supervisor.

---

## Color Palettes

Define 4–8 named color palettes. Each palette belongs to a context: a character, a faction, a location type, a story phase, or an emotional state.

For each palette:

### [Palette Name] — [What it represents]
- **Primary colors:** [specific color names / hex-style descriptions]
- **Accent colors:** [supporting tones]
- **When to use:** [which scenes, characters, or emotional beats this palette applies to]
- **Lighting temp:** [warm / cool / neutral — and intensity level]
- **Mood:** [what this palette communicates emotionally]

---

## Visual Motifs

List 5–10 recurring visual elements that define the show's imagery. For each:

**[Motif name]**
- What it is: [physical description]
- Where it appears: [which scenes, characters, or locations]
- What it means: [its narrative or thematic function]
- How to render it: [practical image generation note — angle, framing, lighting]

---

## Emotional Registers

Define 4–6 emotional registers the show operates in. Each register is a distinct visual mode.

### [Register Name] — e.g. "Quiet Menace", "Triumphant Ascent", "Shattered Intimacy"
- **Visual cues:** [lighting, framing, color temp, depth of field]
- **Character behavior:** [posture, expression, movement]
- **Environment:** [how the setting changes or contributes]
- **Typical scenes:** [what kinds of moments use this register]

---

## Cinematography Notes

General rules for how this show looks. Write 8–15 bullet points covering:
- Preferred shot distances (close-up heavy? wide establishing shots?)
- Camera angle tendencies (low angle for power? Dutch tilt for unease?)
- Depth of field style (shallow for intimacy? deep for world-building?)
- Transition tendencies (hard cuts? slow dissolves?)
- Any show-specific visual rules
- How action scenes differ from dialogue scenes visually
- How internal/emotional moments are externalized visually

---

## Style Anchor

This line must appear identically in every shot's Style field:

> Style: cinematic, photorealistic, 8K, shallow depth of field, single-frame still image, high detail, film lighting

---

## OUTPUT RULES

1. Do not add any preamble, explanation, or commentary outside the structure above.
2. Do not truncate. Every section must be complete.
3. Base every palette, motif, and register on evidence from the scripts — do not invent elements that aren't present.
4. Output only the markdown document. Nothing before or after it.
""",

"character_canvas": """\
OUTPUT CONSTRAINT: Produce a complete but concise document. Avoid padding. Under 1500 words.

---

You are a professional character visual coordinator for an AI-assisted video content pipeline. Your job is to read all available episode scripts for a show and produce a comprehensive Character Canvas in markdown format.

This file is a permanent show-level reference. Every named character who appears across the show must be documented here with enough visual precision that their appearance can be reproduced consistently across all image generation calls, in any episode, across any scene.

---

## OUTPUT FORMAT

---

# Character Canvas — [Show Name]

## Cast Overview
One paragraph listing all documented characters and their broad role in the show.

---

## Character Profiles

For every named character in the show, produce a full profile using this structure:

---

### [Character Full Name]
**Role:** [Protagonist / Antagonist / Supporting / Mentor / etc.]
**First appears:** [Episode number]

#### Physical Description
| Feature | Description |
|---|---|
| Height & Build | [e.g. tall, lean athletic build] |
| Skin tone | [specific — e.g. deep brown, warm medium tan] |
| Hair | [color, texture, length, typical styling] |
| Eyes | [color and shape] |
| Face | [notable features] |
| Distinguishing marks | [scars, birthmarks, tattoos, other permanent physical markers] |

#### Default Outfit (Signature Look)
- **Top:** [specific garment, color, material, fit]
- **Bottom:** [specific garment, color, material, fit]
- **Footwear:** [specific]
- **Outerwear:** [if applicable]
- **Accessories:** [jewelry, bags, hats, belts — describe each item's position, size, color, material]

#### Recurring Outfit Variations
List any other outfits this character wears regularly.

#### Key Visual Tells
3–5 bullet points. Visual shortcuts that make this character instantly recognizable even in a close-up.

#### Character Arc Note
1–2 sentences on how this character's visual presentation changes across the show.

---

## Relationship Map
| Character A | Relationship | Character B |
|---|---|---|
| [Name] | rival / mentor / lover / enemy / sibling | [Name] |

---

## OUTPUT RULES

1. Do not add any preamble, explanation, or commentary outside the structure above.
2. Do not truncate. Document every named character who appears in the scripts.
3. All descriptions must be visual and physical only — precise enough for image generation.
4. If a character's appearance is not described, infer from role/background and flag with *(inferred)*.
5. Output only the markdown document. Nothing before or after it.
""",

"location_reference": """\
You are a professional production designer for an AI-assisted video content pipeline. Your job is to read all available episode scripts for a show and produce a comprehensive Location Reference File in markdown format.

This file is a permanent show-level reference. Every distinct location that appears across the show must be documented here with enough visual precision that it can be reproduced consistently across all image generation calls.

---

## OUTPUT FORMAT

---

# Location Reference — [Show Name]

## Location Overview
One paragraph listing all documented locations grouped by category.

---

## Location Profiles

For every distinct location in the show:

---

### [Location Name]
**Type:** [Interior / Exterior / Mixed]
**Category:** [e.g. Institutional, Domestic, Urban Street, Natural Environment]
**First appears:** [Episode number]
**Recurring:** [Yes / No / Occasional]

#### Physical Description
- Size and scale
- Key architectural or environmental features
- Condition
- Key furniture or fixed elements
- Entry/exit points and how they're used dramatically

#### Lighting Profile
| Time of Day | Light Source | Quality | Color Temp |
|---|---|---|---|
| Day | [Natural / Artificial / Mixed] | [Harsh / Soft / Dappled] | [Warm / Cool / Neutral] |
| Night | [Natural / Artificial / Mixed] | [Harsh / Soft / Glowing] | [Warm / Cool / Neutral] |

#### Atmosphere & Mood
- **Default mood:** [what this location feels like before any characters enter]
- **Color palette:** [dominant colors of the space]
- **Sound implied:** [what sounds define this space]
- **Emotional register:** [what emotional state this location typically hosts]

#### Dramatic Usage
- Which characters use this location and in what context
- What kinds of scenes happen here
- Any scenes where the visual character changes significantly

#### Image Generation Notes
- Wide establishing shot framing notes
- Recurring close-up elements unique to this location
- Visual elements that must appear in every shot set here

---

## OUTPUT RULES

1. Do not add any preamble or commentary outside the structure above.
2. Do not truncate. Document every distinct location, including one-time appearances.
3. All descriptions must be visual and physical — precise enough for image generation.
4. Infer from context where details are sparse and flag with *(inferred)*.
5. Output only the markdown document.
""",

"episode_detail": """\
You are a professional production coordinator for an AI-assisted video content pipeline. Your job is to read an episode script and produce a structured Episode Detail File in markdown format.

This file is used as context for AI image generation (Gemini). Every section must be specific, visual, and precise — vague descriptions are useless downstream.

---

## OUTPUT FORMAT

---

# Episode Detail File — [Episode Number]: [Episode Title]

## Episode Summary
2–4 sentences. Cover what happens, the emotional core, and how the episode ends.

---

## Key Characters

| Character | Role in Episode | Outfit Description | Key Visual Tells |
|---|---|---|---|
| [Name] | [What they do / their arc this episode] | [Full outfit: top, bottom, footwear, accessories — be specific about colors, materials, fit] | [Physical markers: hair, build, distinguishing features, recurring accessories] |

Rules:
- Outfit must be specific enough to recreate consistently. "Blue dress" is not enough. "Floor-length cobalt silk dress with off-shoulder neckline, fitted bodice, no jewelry" is correct.
- If a character changes outfits, create a separate row per outfit labeled "Outfit A", "Outfit B".
- Include every character who appears on screen, even briefly.

---

## Key Locations

| Location | Description | Lighting / Atmosphere | Color Temperature |
|---|---|---|---|
| [Location name] | [Physical description: size, layout, key features] | [Time of day, natural vs artificial light, mood] | [Warm / cool / neutral] |

---

## Key Props

| Prop | Description | Scene Context |
|---|---|---|
| [Prop name] | [Precise physical description: size, shape, color, material, condition] | [Which scene and its significance] |

Only include props that are visually prominent or narratively significant.

---

## Key Visual Moments

List the 5–10 most visually distinct and emotionally significant moments.

**[Moment number]. [Brief title]**
- Scene: [where it happens]
- Characters: [who is present]
- What happens visually: [describe as if describing a still image]
- Why it matters: [narrative or emotional significance]

---

## Tone Arc

- **Opening tone:** [e.g. tense, playful, melancholic — and what creates that feeling visually]
- **Mid-episode shift:** [what changes and how it registers on screen]
- **Closing tone:** [how the episode ends emotionally]

---

## Continuity Flags

List details that must stay consistent with prior or future episodes. If none, write: *No continuity flags for this episode.*

---

## Shot Description Notes

3–6 bullet points:
- Visual effects or non-realistic elements needing translation into physical visible cues
- Scenes where close-ups are essential
- Scenes where environment must dominate
- Recurring visual motifs in this episode
- Crowd / large cast framing notes

---

## OUTPUT RULES

1. Do not add any preamble or commentary outside the structure above.
2. Do not truncate. Every section must be complete.
3. Write in present tense throughout.
4. All descriptions must be visual and physical — no metaphor in description fields.
5. Output only the markdown document.
""",

}  # end PROMPTS


# ── Embedded prompts — Stage 2 ────────────────────────────────────────────────

PIPELINE_INSTRUCTIONS = """\
# Shot Breakdown Pipeline — Project Instructions

## Role
You are a professional shot breakdown assistant for AI image generation. Your job is to take raw scripts from any show or IP and transform them into fully structured, production-ready shot breakdown sheets — row by row, exactly like a film storyboard supervisor would.

You understand cinematography, visual storytelling, location design, and AI image generation (Gemini). You always apply the RULEBOOK to every breakdown. Every Shot Description you write must be a GenAI-ready prompt specific enough for Gemini to generate a single still image without ambiguity.

---

## The 5-Step Pipeline

Always follow these steps in order. Never skip a step.

---

### Step 1 — Script Chunker
Break the raw script into shots based on visual logic — not sentence count, not timing.

**The core rule: A shot = one distinct visual moment that can be rendered as a single still image by Gemini.**

**THE IMAGE LOGIC RULE — THE MOST IMPORTANT RULE IN THIS STEP:**
Each shot will become one generated image. The question is never "how long does this play?" — it is: "does this beat deserve its own image?"

Split aggressively. Every time the script introduces a new subject, a new action, a new emotion, or a new visual detail that would look different as a still image — that is a new shot. When in doubt, split.

**LENGTH RULE — APPLY BEFORE ANYTHING ELSE:**
If a chunk of text is more than 30 words, it almost certainly contains more than one visual moment. Stop. Re-read it clause by clause and split it.

A single shot line should describe one thing happening to one subject in one emotional register. If you can identify two subjects, two actions, or two emotions in the same line — it must be split.

**HOW TO SPLIT A LONG SENTENCE:**
A sentence can contain multiple visual moments even if it has no paragraph break. Split at every clause that introduces something new — a new subject, a new action, a new emotion.

Example:
✗ One shot (too long — three distinct visual moments crammed together):
"She had finally made her decision, her eyes cold and resolved. He stood in the doorway, blocking her path, arms crossed and jaw tight. Neither of them moved, the silence between them heavy with everything unsaid."

✓ Correctly split into three shots:
- Shot A: "She had finally made her decision, her eyes cold and resolved." — Her face. The internal shift from doubt to certainty.
- Shot B: "He stood in the doorway, blocking her path, arms crossed and jaw tight." — His body filling the frame.
- Shot C: "Neither of them moved, the silence between them heavy with everything unsaid." — Two figures. The standoff.

**ASK FOR EVERY SENTENCE OR CLAUSE:**
- Does this introduce a new visual subject? → new shot
- Does this show a different action or physical state than the line before? → new shot
- Does this shift the emotional register? → new shot
- Does this describe a new environment, detail, or atmospheric element? → new shot
- Does this line contain more than 30 words? → almost certainly needs splitting
- Would Gemini produce the same image for this line as the previous one? → only then keep grouped

**TARGET SHOT COUNT — USE AS A SELF-CHECK, NOT A CUTTING RULE:**
A well-split script produces approximately 7–9 shots per 100 words. For a 1000-word script: 70–90 shots.

**CRITICAL: Copy the exact original script text for each chunk — word for word, no paraphrasing.**

---

### Step 2 — Object & Action Mapping
For each shot line from Step 1, identify:
- **Objects:** Key visual elements, props, characters, creatures, vehicles
- **Actions:** What is physically happening in the frame
- **Character anchor:** Visual identifiers per Rule 14

---

### Step 3 — Camera Assignment
For each shot, assign:

**Shot Size:** Extreme Close-up | Close-up | Medium Close-up | Medium | Wide | Wide (Profile) | Wide tracking | Extreme Wide | POV | Over-the-shoulder

**Camera Angle:** Eye-level | Low angle | High angle | Dutch angle | Bird's eye | Worm's eye

**Camera Movement:** Static | Pan | Tilt | Dolly in | Dolly out | Track | Handheld | Crane up | Crane down

**Shot Description** must follow the 8-part formula from Rule 12:
[Scene Type] + [Character State] + [Action Moment] + [Environment Reaction] + [Camera Language] + [Lighting] + [Lens] + [Mood]

**SENSORY & POV SHOTS — CRITICAL RULE:**
When the script describes something a character experiences internally — pain, light flashing, sound hitting them — the Shot Description must always anchor to the character's physical face or body. The internal experience is rendered as a visible effect on them, never as a standalone abstract image.

**OUTFIT RULE — CRITICAL:**
The character's outfit must be stated explicitly inside the Shot Description prompt itself. Every Shot Description that includes a character must contain their outfit.

**BODY POSITION ANCHOR ON CLOSE-UPS — CRITICAL:**
Even on extreme close-ups, state: (1) the character's full body position, (2) a soft background anchor describing the location.

**ABSTRACT CONCEPT RULE — CRITICAL:**
Internal mental events must be translated into physical reactions on the character's face or body only. Never use abstract visual metaphors, overlays, or projections.

Assign **Intensity Level** (1–4) to each shot:
- Level 1 = Social humiliation / public shame
- Level 2 = Internal conflict / emotional hesitation
- Level 3 = Power activation / transformation beginning
- Level 4 = Peak power / monster state / full transformation

---

### Step 4 — Location & Environment Setup
For each shot:
- **Environment:** Short location name, 1–3 words
- **Time of Day:** Dawn / Morning / Day / Dusk / Night
- **Lighting:** Specific — apply Color Strategy from Rule 5
- **Background:** What is visible behind the subject
- **Environment Reaction:** What the environment is doing (Rule 7)
- **Color Palette:** Dominant colors matching the emotional beat
- **Mood:** Atmospheric quality

---

### Step 5 — Continuity Validator
Check: object/character continuity, location consistency, camera flow, rulebook compliance (all 18 rules), prompt quality.

Report: ✓ working well, ⚠️ issues (shot number + problem), 🔧 recommended fix, 🚩 rulebook violations.
**Readiness Score: X/100**

---

## Final Output — The Shot Breakdown Sheet

After all 5 steps, produce the final sheet.

**IMPORTANT OUTPUT FORMAT:**
When asked to produce the Excel output (triggered by "MAKE EXCEL"), output each batch of 25 shots as a JSON array wrapped in ```json and ``` markers. Each shot object must have exactly these keys:
- shot_number (integer)
- line (string — exact original script text, verbatim)
- shot_size (string)
- shot_description (string — full 8-part GenAI prompt)
- shot_detail (string — Style Anchor + Part 1 bullets + Character & Location Details + Show Tone, newlines as \\n)
- reference (string — camera angle)

End each batch with the text: BATCH COMPLETE — shots [X]–[Y] done.
After the final batch: ALL SHOTS COMPLETE — [X] total shots delivered.

---

## Shot Detail Column Structure

Every shot_detail value must contain three parts in this exact order:

**STYLE ANCHOR (first line, every shot, no exceptions):**
Style: cinematic, photorealistic, 8K, shallow depth of field, single-frame still image, high detail, film lighting

**PART 1 — Shot Breakdown Bullets:**
• Location -> [location name]
• Character -> [CHARACTER NAME] | Action: [what they are physically doing] | Emotion: [what they are feeling]
• Character State -> [visible expression or body language]
• Lighting -> [light description]
• Camera Angle ([angle]) + Camera Movement ([movement]) -> [plain English description]
• Lens -> [lens]
• Mood -> [atmosphere phrase]

**PART 2 — Character & Location Details:**
[CHARACTER NAME]: [build + age], [hair], [face/eyes], wearing [full outfit — exact garments, colors, fabric, fit], [wounds/marks if visible], [pose/action in this shot]

[LOCATION NAME]: [brief type descriptor]. [Architecture/features]. [Time of day], [light source and quality], [color temperature]. Background: [what is visible]. [Atmosphere or "No atmospheric effects"]. Mood: [one evocative phrase].

**PART 3 — Show Tone:**
Color Palette: [derived from loaded show tone file — specific colors, quality of light, color temperature]
Visual Motif: [specific motif from show tone file, or "None"]
Emotional Register: [tone state from show tone file — specific phrase tied to this shot's beat]

---

## How to Trigger the Pipeline

**STAGE 1 — BREAKDOWN** (triggered by pasting a raw script):
Run all 5 steps internally. When complete, output ONLY:
```
BREAKDOWN COMPLETE
Episodes analysed: [episode name]
Total shots: [X]
Rulebook violations: [X — list briefly, or "None"]
Ready for output. Type MAKE EXCEL to generate the sheet.
```

**STAGE 2 — OUTPUT** (triggered by "MAKE EXCEL"):
Output 25 shots at a time as JSON arrays (see format above). Wait for "NEXT" between batches.

**OTHER COMMANDS:**
- "STEP 1 ONLY" — run and output Step 1 chunk list only
- "VALIDATE" — run Step 5 on current breakdown
"""


RULEBOOK = """\
# GenAI Cinematic Promo Rulebook
Fantasy / Action IP | Social-First | Meta Optimized | High-Retention Structure

---

## 1. CORE OBJECTIVE

Primary Goal — Maximize: 3-sec plays, 15-sec Thruplays, hook retention, emotional escalation, curiosity gap.

Target Audience: USA | 18–55 | Blue-collar mindset — emotion-driven, direct stakes, power fantasy, betrayal/revenge/underdog arcs.

---

## 2. STRUCTURE RULES

### Rule 1: Start With Conflict, Not Context
Never open with exposition. First shot must be: humiliation, violence/physical threat, public embarrassment, power reveal, betrayal, or imminent danger. Hook must visually escalate in the first 2.5 seconds.

### Rule 2: Every 3–5 Seconds = Escalation
No flat emotional plateaus. Each beat must increase: danger, humiliation, power, chaos, or mystery. If nothing changes — cut it.

### Rule 3: Character Framing Hierarchy
| Power State | Camera Rule |
|---|---|
| Dominant / powerful | Low angle |
| Vulnerable / weak | High angle |
| Turning point / gaining power | Eye-level push-in |
| Fully transformed / peak power | Extreme close-up + shadow |
| Two characters at equal tension | Eye-level, static |

---

## 3. SHOT DIVISION RULES

Average shot length: 1.2–2.5 seconds. Action beats: 0.5–1 second. Emotional beats: 2–3 seconds max. No shots longer than 3 seconds without escalation.

---

## 4. DIALOGUE RULES

Dialogue must cut like a punch. Short. Punchy. Each word lands. Layer: primary dialogue + system/inner voice + crowd chant + low sub bass.

---

## 5. COLOR STRATEGY

| Emotional Beat | Color Direction |
|---|---|
| Humiliation | Cold blue |
| Betrayal | Desaturated / washed out |
| Rage | Warm red / orange |
| Power activation | High contrast + glow |
| Calm dominant character | Neutral with dramatic shadow contrast |
| Mystery / unknown threat | Dark, low saturation, single accent color |

---

## 6. POWER REVEAL — MANDATORY BUILD ORDER
1. Micro physical twitch or reaction
2. Sound distortion
3. Steam / heat / energy emanating from body
4. Extreme close-up on eyes
5. Environmental reaction (objects move, lights flicker, crowd reacts)
6. Full payoff (transformation, ability, destruction)
Never jump straight to the payoff.

---

## 7. ENVIRONMENT CHAOS RULE
The environment is a character. It must react. Every high-intensity shot must include at least one: wind whipping clothing/hair, dust/debris lifting, sparks/embers floating, smoke/steam in frame, flickering lights. If the environment is still — the image feels fake.

---

## 8. DOMINANT CALM CHARACTER RULE
Minimal physical movement. Controlled expression. Shadow framing. Reacts last, speaks least. Should look dangerous without trying. They feel mythic — the scene bends around them.

---

## 9. TRANSITION RULEBOOK
Rage → Chaos: environmental element morphs into next scene.
Close-up eye → Memory: cut on blink.
Crowd chant → Internal voice: crowd audio fades into inner whisper.
Steam Match Cut: end shot steam fills frame → next shot smoke clearing.
Eye Glow Transition: extreme close-up glowing eye → match cut to same color light source.

---

## 10. META PERFORMANCE RULES

First 3 seconds — must show at least one: character with clear power, character being publicly humiliated, imminent physical danger.

0–15 seconds — must include: 2+ visible power escalations, 1 emotional betrayal/stakes moment, 1 unresolved curiosity hook.

End Rule: Always end on unresolved tension. Never full resolution.

---

## 11. PROTAGONIST ARC RULE
Lead character must visually pass through these states in order:
1. Weak / normal
2. Humiliated / threatened
3. Pushed to the edge
4. Overwhelmed / breaking point
5. Dangerous / transformed

---

## 12. GENAI PROMPT FORMULA
[Scene Type] + [Character State] + [Action Moment] + [Environment Reaction] + [Camera Language] + [Lighting] + [Lens] + [Mood]

Intensity Levels:
- Level 1 — Social Humiliation: cold lighting, crowd, desaturation
- Level 2 — Internal Conflict: tight close-up, heat distortion, tension building
- Level 3 — Power Activation: steam, sparks, energy glow, environmental reaction
- Level 4 — Peak Power/Monster State: extreme close-up, face in shadow, transformation detail

---

## 13. CAMERA LANGUAGE BY POWER STATE

Dominant/Powerful: low-angle, subtle slow push-in, sharp focus, strong jawline shadow.
Vulnerable/Weak: slightly high-angle, wider frame, softer lighting, background overpowering subject.
Turning Point: eye-level, slow push-in, shallow depth of field, background fades, light catching eyes.

---

## 14. CHARACTER VISUAL CONSISTENCY RULE
Always repeat in every prompt: hair style/color, clothing color/style, eye color, physical build, signature accessory. Never rephrase core character identifiers shot-to-shot.

---

## 15. EDIT-AWARE PROMPTING RULE
Avoid: ultra-wide scenic compositions, slow static framing, character standing doing nothing.
Prefer: mid-action moments, tight faces with expression, movement implied in frame, cropped tension.
Rule: "The frame should feel like it's already in motion."

---

## 16. GENAI QUALITY CONTROL CHECKLIST
- [ ] Does it show movement or implied motion?
- [ ] Does lighting reflect the emotional state?
- [ ] Is power hierarchy visible through camera angle?
- [ ] Is the environment reacting?
- [ ] Is this scroll-stopping within 1 second?
If 2 or more answers are weak — regenerate.

---

## 17. PROMO ARC PROMPT FLOW

| Beat | Intensity Level | Key Elements |
|---|---|---|
| Opening humiliation | Level 1 | Cold lighting, crowd, desaturation |
| Crowd chaos | Level 1–2 | Handheld, chanting, phones recording |
| Internal conflict | Level 2 | Tight frame, heat distortion |
| Power activation begins | Level 3 | Steam, sparks, energy glow |
| Eyes reveal | Level 3–4 | Extreme close-up, glowing eyes |
| Full transformation | Level 4 | Shadow dominance, extreme close-up |
| Threat / final line | Level 4 | Low angle, controlled power, unresolved |

---

## 18. FINAL CHECKLIST BEFORE EXPORT
- [ ] Hook lands in first 2 seconds — conflict, power, or humiliation
- [ ] No shot exceeds 3 seconds without escalation
- [ ] Power reveal builds across 6 stages — never instant
- [ ] Environment reacts in every high-intensity shot
- [ ] Dominant calm character feels mythic, not loud
- [ ] End leaves unresolved tension — no closure
- [ ] Every Shot Description follows the 8-part prompt formula
- [ ] Character visual identifiers are consistent across all shots
"""

STAGE2_SYSTEM = f"{PIPELINE_INSTRUCTIONS}\n\n---\n\n{RULEBOOK}"


# ── Supabase helpers ─────────────────────────────────────────────────────────

def _supabase(method: str, path: str, data=None, params=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY not set in .env")
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }
    r = httpx.request(
        method, f"{SUPABASE_URL}/rest/v1{path}",
        headers=headers, json=data, params=params, timeout=30,
    )
    r.raise_for_status()
    return r.json() if r.content else []


def _classify_file_type(filename: str) -> str:
    n = filename.lower()
    if "show_tone_bible" in n or "tone_bible" in n: return "show_tone_bible"
    if "character_canvas" in n:                      return "character_canvas"
    if "location_reference" in n:                    return "location_reference"
    if "episode_detail" in n:                        return "episode_detail"
    return "other"


# ── Flask app + job store ─────────────────────────────────────────────────────

app  = Flask(__name__)
jobs: dict = {}


# ── Per-thread logging ────────────────────────────────────────────────────────

_job_local  = threading.local()
_print_lock = threading.Lock()


def _log(msg: str):
    with _print_lock:
        print(msg, flush=True)
    q = getattr(_job_local, "queue", None)
    if q is not None:
        q.put(str(msg))


def _set_job_context(job_id: str, q):
    _job_local.job_id = job_id
    _job_local.queue  = q


# ── API helpers ───────────────────────────────────────────────────────────────

def _track_tokens(data: dict):
    usage = data.get("usage", {})
    jid   = getattr(_job_local, "job_id", None)
    if jid and jid in jobs:
        with jobs[jid]["tokens_lock"]:
            jobs[jid]["tokens"]["input"]  += usage.get("prompt_tokens", 0)
            jobs[jid]["tokens"]["output"] += usage.get("completion_tokens", 0)
            jobs[jid]["tokens"]["calls"]  += 1


def verify_auth():
    _log("  Verifying API credentials...")
    if not ARGUS_API_KEY or not ARGUS_BASE_URL:
        raise RuntimeError("ARGUS_API_KEY or ARGUS_BASE_URL not set in .env")
    try:
        r = httpx.post(
            ARGUS_BASE_URL,
            headers={"Authorization": f"Bearer {ARGUS_API_KEY}", "Content-Type": "application/json"},
            json={"model": ARGUS_MODEL, "messages": [{"role": "user", "content": "Reply with one word: READY"}], "max_tokens": 10},
            timeout=60,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        if text:
            _log(f"  ok  (model: {ARGUS_MODEL})")
            return
        raise RuntimeError("Empty response from API")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise RuntimeError("Invalid API key")
        raise RuntimeError(f"Auth check failed: {e.response.status_code}")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Auth check failed: {e}")


def call_api(prompt: str, label: str = "", timeout=None, retries: int = 2) -> str:
    """Single-turn API call (Stage 1)."""
    label_str = f"[{label}] " if label else ""
    headers   = {"Authorization": f"Bearer {ARGUS_API_KEY}", "Content-Type": "application/json"}
    payload   = {"model": ARGUS_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4096}

    for attempt in range(1, retries + 2):
        _log(f"  ... {label_str}attempt {attempt}/{retries + 1}...")
        try:
            r = httpx.post(ARGUS_BASE_URL, headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            text = data["choices"][0]["message"]["content"].strip()
            if text:
                _log(f"  ok  {label_str}done")
                _track_tokens(data)
                return text
            _log("  x  Empty response")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                _log(f"  !!  Rate limit — waiting {RATE_LIMIT_WAIT}s...")
                time.sleep(RATE_LIMIT_WAIT)
            elif e.response.status_code == 401:
                raise RuntimeError("Invalid API key")
            else:
                body = e.response.text[:400] if e.response.text else ""
                _log(f"  x  HTTP {e.response.status_code}  {body}")
        except httpx.TimeoutException:
            _log(f"  x  Connection/read timeout")
        except Exception as e:
            _log(f"  x  {e}")

        if attempt <= retries:
            wait = 20 * attempt
            _log(f"  -> Retrying in {wait}s...")
            time.sleep(wait)

    _log(f"  x  All attempts failed: {label}")
    return ""


def call_api_chat(
    system: str, messages: list,
    label: str = "", timeout=None, retries: int = 2,
) -> str:
    """Multi-turn streaming chat API call (Stage 2)."""
    label_str = f"[{label}] " if label else ""
    headers   = {"Authorization": f"Bearer {ARGUS_API_KEY}", "Content-Type": "application/json"}
    payload   = {
        "model":         ARGUS_MODEL,
        "messages":      [{"role": "system", "content": system}] + messages,
        "max_tokens":    8192,
        "stream":        True,
        "stream_options": {"include_usage": True},
    }

    for attempt in range(1, retries + 2):
        _log(f"  ... {label_str}attempt {attempt}/{retries + 1} (streaming)...")
        try:
            with httpx.stream("POST", ARGUS_BASE_URL, headers=headers, json=payload, timeout=timeout) as r:
                if r.status_code >= 400:
                    body = r.read().decode("utf-8", errors="replace")[:400]
                    if r.status_code == 429:
                        _log(f"  !!  Rate limit — waiting {RATE_LIMIT_WAIT}s...")
                        time.sleep(RATE_LIMIT_WAIT)
                    elif r.status_code == 401:
                        raise RuntimeError("Invalid API key")
                    else:
                        _log(f"  x  HTTP {r.status_code}  {body}")
                else:
                    full_text    = ""
                    usage        = None
                    started      = time.time()
                    last_log     = started
                    got_first    = False

                    for raw in r.iter_lines():
                        line = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                        if not line or not line.startswith("data:"):
                            continue
                        data_str = line[5:].strip()
                        if data_str == "[DONE]":
                            break
                        try:
                            chunk_data = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue

                        choices = chunk_data.get("choices") or []
                        if choices:
                            delta = choices[0].get("delta") or {}
                            piece = delta.get("content") or ""
                            if piece:
                                if not got_first:
                                    _log(f"  ... {label_str}streaming started")
                                    got_first = True
                                full_text += piece

                        if chunk_data.get("usage"):
                            usage = chunk_data["usage"]

                        now = time.time()
                        if now - last_log > 12 and got_first:
                            _log(f"  ... {label_str}{len(full_text):,} chars streamed ({int(now - started)}s)")
                            last_log = now

                    if full_text.strip():
                        _log(f"  ok  {label_str}done ({len(full_text):,} chars in {int(time.time() - started)}s)")
                        if usage:
                            _track_tokens({"usage": usage})
                        else:
                            # Rough fallback when upstream doesn't emit usage: ~4 chars/token
                            _track_tokens({"usage": {
                                "prompt_tokens":     0,
                                "completion_tokens": len(full_text) // 4,
                            }})
                        return full_text.strip()
                    _log(f"  x  {label_str}empty stream")

        except httpx.TimeoutException:
            _log(f"  x  Connection/read timeout")
        except RuntimeError:
            raise
        except Exception as e:
            _log(f"  x  {e}")

        if attempt <= retries:
            wait = 20 * attempt
            _log(f"  -> Retrying in {wait}s...")
            time.sleep(wait)

    _log(f"  x  All attempts failed: {label}")
    return ""


# ── Upload text extraction (handles .docx) ───────────────────────────────────

def _read_upload_as_text(file_storage) -> str:
    """Read an uploaded FileStorage as text. Converts .docx → markdown via mammoth."""
    filename = (file_storage.filename or "").lower()
    raw      = file_storage.read()
    if filename.endswith(".docx"):
        try:
            import io, mammoth
            return mammoth.convert_to_markdown(io.BytesIO(raw)).value.strip()
        except ImportError:
            raise RuntimeError("pip install mammoth — needed to read .docx")
        except Exception as e:
            raise RuntimeError(f"Failed to convert {file_storage.filename}: {e}")
    return raw.decode("utf-8", errors="replace")


# ── Stage 1 helpers ───────────────────────────────────────────────────────────

def _convert_docx(src: Path, dest: Path) -> bool:
    try:
        import mammoth
        with open(src, "rb") as f:
            res = mammoth.convert_to_markdown(f)
        text = res.value.strip()
        if not text:
            _log(f"  !  Empty output for {src.name} — skipping")
            return False
        dest.write_text(text, encoding="utf-8")
        return True
    except ImportError:
        _log("  x  pip install mammoth  (needed for .docx)")
        return False
    except Exception as e:
        _log(f"  x  {src.name}: {e}")
        return False


def auto_convert(scripts_dir: Path):
    to_convert = [
        f for ext in ["*.docx", "*.txt", "*.fountain"]
        for f in scripts_dir.glob(ext)
        if not f.with_suffix(".md").exists()
    ]
    if not to_convert:
        return
    _log(f"  Converting {len(to_convert)} script(s) to .md...")
    for src in to_convert:
        dest = src.with_suffix(".md")
        if src.suffix == ".docx":
            ok = _convert_docx(src, dest)
        else:
            try:
                dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
                ok = True
            except Exception as e:
                _log(f"  x  {src.name}: {e}")
                ok = False
        _log(f"  {'ok' if ok else 'x '}  {src.name} -> {dest.name}")


def load_prompt(name: str) -> str:
    if name not in PROMPTS:
        raise RuntimeError(f"Unknown prompt: {name}")
    return PROMPTS[name]


def read_scripts(scripts_dir: Path, filenames: list = None) -> dict:
    if filenames:
        files   = [scripts_dir / f for f in filenames]
        missing = [str(f) for f in files if not f.exists()]
        if missing:
            raise RuntimeError(f"Scripts not found: {missing}")
    else:
        files = sorted(scripts_dir.glob("*.md"))
        if not files:
            raise RuntimeError(f"No .md scripts in: {scripts_dir}")
    return {f.stem: f.read_text(encoding="utf-8") for f in files}


def trim(text: str, max_chars: int = 30_000) -> str:
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars].rfind("\n\n")
    if cut < max_chars * 0.7:
        cut = max_chars
    return text[:cut] + "\n\n[...trimmed...]"


def call_claude(system: str, user: str, label: str) -> str:
    brevity = "OUTPUT CONSTRAINT: Complete but concise. Under 1500 words. No padding.\n\n"
    prompt  = f"{brevity}{system}\n\n---\n\n{user}"
    return call_api(prompt, label=label, retries=2)


# ── Stage 1 batch helpers ─────────────────────────────────────────────────────

def batch_label(ep_stems: list) -> str:
    return f"{ep_stems[0]}-{ep_stems[-1]}"


def show_level_filename(file_type: str, label: str) -> str:
    return f"{file_type}_{label}.md"


def find_previous_batch_file(file_type: str, current_label: str, out_dir: Path) -> str:
    existing      = sorted(out_dir.glob(f"{file_type}_*.md"))
    current_fname = show_level_filename(file_type, current_label)
    prior         = [f for f in existing if f.name != current_fname]
    return prior[-1].read_text(encoding="utf-8") if prior else ""


# ── Stage 1 generation ────────────────────────────────────────────────────────

def generate_show_level_batch(file_type, batch_stems, batch_scripts, all_stems, out_dir, show_name):
    label    = batch_label(batch_stems)
    out_path = out_dir / show_level_filename(file_type, label)

    if out_path.exists() and out_path.stat().st_size > 100:
        _log(f"  ->  {out_path.name} — already exists, skipping")
        return

    prev_content = find_previous_batch_file(file_type, label, out_dir)
    prev_block   = ""
    if prev_content:
        prev_block = (
            f"\n\n## INHERITED FROM PREVIOUS BATCHES\n"
            f"The following is already established. Extend and update it — do not contradict it.\n\n"
            f"{trim(prev_content, max_chars=20_000)}\n\n---\n\n"
            f"## NEW EPISODES TO PROCESS (add to the above)\n"
        )

    system   = load_prompt(file_type)
    combined = "\n\n---\n\n".join(
        f"### Script: {s}\n\n{trim(batch_scripts[s], max_chars=3_000)}"
        for s in batch_stems
    )

    total_eps     = len(all_stems)
    batch_num     = all_stems.index(batch_stems[0]) // BATCH_SIZE + 1
    total_batches = (total_eps + BATCH_SIZE - 1) // BATCH_SIZE

    user = (
        f"Show: {show_name}\n"
        f"Processing: Episodes {batch_stems[0]} to {batch_stems[-1]} "
        f"(batch {batch_num}/{total_batches} of {total_eps} total episodes)\n"
        f"{prev_block}{combined}"
    )

    _log(f"  Generating: {out_path.name} ...")
    result = call_claude(system, user, label=f"{file_type} {label}")
    if not result:
        result = f"# {file_type.replace('_', ' ').title()} — {label}\n\n[Generation failed — rerun]"
    out_path.write_text(result, encoding="utf-8")
    _log(f"  ok  {out_path.name}  ({len(result):,} chars)")


def generate_all_show_level_batches(scripts, out_dir, show_name, file_types=None):
    if file_types is None:
        file_types = ["show_tone_bible", "character_canvas", "location_reference"]
    all_stems = list(scripts.keys())
    total     = len(all_stems)
    batches   = [all_stems[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    _log(f"\n  {total} episodes -> {len(batches)} batches of {BATCH_SIZE}")
    _log(f"  {len(file_types)} file type(s) x {len(batches)} batches = {len(file_types) * len(batches)} total calls\n")

    for bi, batch_stems in enumerate(batches, 1):
        batch_scripts = {s: scripts[s] for s in batch_stems}
        _log(f"  -- Batch {bi}/{len(batches)}: {batch_label(batch_stems)} --")
        for i, ft in enumerate(file_types):
            generate_show_level_batch(ft, batch_stems, batch_scripts, all_stems, out_dir, show_name)
            if i < len(file_types) - 1:
                time.sleep(INTER_CALL_PAUSE)
        if bi < len(batches):
            time.sleep(INTER_CALL_PAUSE)

    _log("\n  ok  Show-level files complete.")


def _episode_detail_task(script_stem, script_content, out_dir, show_name, worker_idx, log_queue, job_id):
    _set_job_context(job_id, log_queue)
    out_path = out_dir / f"{script_stem}_episode_detail.md"
    if out_path.exists() and out_path.stat().st_size > 100:
        _log(f"  ->  [{worker_idx}] {script_stem} — skip (exists)")
        return script_stem, True, out_path.stat().st_size

    system = load_prompt("episode_detail")
    user   = f"Show: {show_name}\nEpisode: {script_stem}\n\n---\n\n{trim(script_content)}"
    _log(f"  ... [{worker_idx}] {script_stem} ...")
    result = call_claude(system, user, label=script_stem)

    if not result:
        result = f"# {script_stem} Episode Detail\n\n[Generation failed — rerun]"
        _log(f"  x  [{worker_idx}] {script_stem} — failed")
        out_path.write_text(result, encoding="utf-8")
        return script_stem, False, 0

    out_path.write_text(result, encoding="utf-8")
    _log(f"  ok  [{worker_idx}] {script_stem}  ({len(result):,} chars)")
    return script_stem, True, len(result)


def generate_all_episode_details(scripts, out_dir, show_name, workers, log_queue, job_id):
    items     = list(scripts.items())
    total     = len(items)
    skipped   = sum(
        1 for s, _ in items
        if (out_dir / f"{s}_episode_detail.md").exists()
        and (out_dir / f"{s}_episode_detail.md").stat().st_size > 100
    )
    _log(f"\n  {total} episodes  |  {skipped} already done  |  {total - skipped} to generate")
    _log(f"  Workers: {workers} parallel  |  Stagger: {STAGGER_SEC}s\n")

    if total - skipped == 0:
        _log("  ok  All episode detail files already exist.")
        return

    done = failed = 0
    start = time.time()

    if job_id in jobs:
        jobs[job_id]["progress"]["total"]      = total
        jobs[job_id]["progress"]["completed"]  = skipped
        jobs[job_id]["progress"]["started_at"] = start
        jobs[job_id]["progress"]["stage"]      = "Episode details"

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, (stem, content) in enumerate(items):
            if idx > 0 and idx % workers == 0:
                time.sleep(STAGGER_SEC)
            f = executor.submit(_episode_detail_task, stem, content, out_dir, show_name, (idx % workers) + 1, log_queue, job_id)
            futures[f] = stem

        for future in as_completed(futures):
            _, success, _ = future.result()
            if success: done += 1
            else:       failed += 1
            elapsed = time.time() - start
            eta = (elapsed / max(done + failed, 1)) * (total - done - failed)
            _log(f"  Progress: {done + failed}/{total}  ok:{done}  x:{failed}  ETA ~{eta / 60:.0f}min")
            if job_id in jobs:
                jobs[job_id]["progress"]["completed"] = skipped + done + failed

    _log(f"\n  ok  {done}/{total} done  ({failed} failed)")


def _is_show_level_file(filename: str) -> bool:
    if filename in SHOW_LEVEL_FILES:
        return True
    return any(filename.startswith(stem + "_") for stem in SHOW_LEVEL_STEMS)


def run_stage1_pipeline(job_id: str, job_dir: Path, mode: str, workers: int, episode: str):
    log_queue = jobs[job_id]["queue"]
    _set_job_context(job_id, log_queue)

    scripts_dir = job_dir / "episodic scripts"
    raw_out     = job_dir / "_stage1_out"
    raw_out.mkdir(parents=True, exist_ok=True)
    show_name   = job_dir.name

    try:
        _log(f"\n{'=' * 60}")
        _log("  STAGE 1 — Reference File Generation")
        _log(f"  Show: {show_name}")
        _log(f"{'=' * 60}\n")

        verify_auth()
        auto_convert(scripts_dir)

        filenames = [episode] if mode == "episode" else None
        scripts   = read_scripts(scripts_dir, filenames)

        if mode in ("full", "all-episodes"):
            _log("\n  -- Episode Detail Files ----------------------")
            generate_all_episode_details(scripts, raw_out, show_name, workers, log_queue, job_id)

        if mode in ("full", "show-level"):
            _log("\n  -- Show-Level Files (batched) ----------------")
            generate_all_show_level_batches(scripts, raw_out, show_name)

        if mode == "episode":
            stem = Path(episode).stem
            _log(f"\n  -- Single episode: {stem} --")
            _episode_detail_task(stem, scripts[stem], raw_out, show_name, 1, log_queue, job_id)

        show_level_dir = job_dir / "show level files"
        ep_details_dir = job_dir / "episode details"
        show_level_dir.mkdir(exist_ok=True)
        ep_details_dir.mkdir(exist_ok=True)

        moved_show = moved_ep = 0
        for f in raw_out.glob("*.md"):
            dest = show_level_dir if _is_show_level_file(f.name) else ep_details_dir
            shutil.copy2(f, dest / f.name)
            if dest is show_level_dir: moved_show += 1
            else:                      moved_ep   += 1
        shutil.rmtree(raw_out, ignore_errors=True)

        _log(f"\n{'=' * 60}")
        _log("  STAGE 1 COMPLETE")
        _log(f"    -> show level files/  ({moved_show} file(s))")
        _log(f"    -> episode details/   ({moved_ep} file(s))")
        _log(f"{'=' * 60}\n")

        jobs[job_id]["status"] = "done"

    except Exception as e:
        _log(f"\n[ERROR] {e}")
        jobs[job_id]["status"] = "failed"
    finally:
        log_queue.put(None)


# ── Stage 2 helpers ───────────────────────────────────────────────────────────

def parse_shot_rows(text: str) -> list:
    """Extract shot rows from API response. Tries JSON first, falls back to markdown table."""
    # Try ```json ... ``` block
    m = re.search(r"```json\s*([\s\S]*?)\s*```", text)
    if m:
        try:
            data = json.loads(m.group(1))
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

    # Try bare JSON array
    m = re.search(r"\[\s*\{[\s\S]*?\}\s*\]", text)
    if m:
        try:
            data = json.loads(m.group(0))
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

    # Fallback: markdown table — rows starting with | digit
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if not cells or not cells[0].isdigit():
            continue
        if len(cells) >= 6:
            rows.append({
                "shot_number":      int(cells[0]),
                "line":             cells[1],
                "shot_size":        cells[2],
                "shot_description": cells[3],
                "shot_detail":      cells[4],
                "reference":        cells[5] if len(cells) > 5 else "",
            })
    return rows


def write_excel(rows: list, output_path: Path, episode_name: str):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = episode_name[:30]

    headers = ["#", "Line", "Shot Size", "Shot Description", "Shot Detail", "Reference", "Preview_1", "Preview_2"]

    # Header row
    header_fill = PatternFill("solid", fgColor="1F3864")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font   = header_font
        cell.fill   = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Data rows
    for row_data in rows:
        row_vals = [
            row_data.get("shot_number", ""),
            row_data.get("line", ""),
            row_data.get("shot_size", ""),
            row_data.get("shot_description", ""),
            row_data.get("shot_detail", ""),
            row_data.get("reference", ""),
            "",
            "",
        ]
        ws.append(row_vals)
        r = ws.max_row
        for col in range(1, 9):
            ws.cell(r, col).alignment = Alignment(vertical="top", wrap_text=True)

    # Column widths
    col_widths = [5, 40, 14, 55, 80, 15, 12, 12]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"

    wb.save(str(output_path))


MAKE_EXCEL_MSG = (
    "MAKE EXCEL\n"
    "Output each batch of 25 shots as a JSON array wrapped in ```json and ``` markers. "
    "Each shot object must have exactly these keys: shot_number (integer), line (string), "
    "shot_size (string), shot_description (string), shot_detail (string), reference (string). "
    "End each batch with: BATCH COMPLETE — shots X–Y done. "
    "After the last batch: ALL SHOTS COMPLETE — X total shots delivered."
)


def _build_stage2_user_msg(ref_files: dict, script_text: str, episode_name: str) -> str:
    ref_parts = [
        f"## {k.replace('_', ' ').title()}\n\n{v.strip()}"
        for k, v in ref_files.items() if v and v.strip()
    ]
    ref_context = "\n\n---\n\n".join(ref_parts)
    if ref_context:
        return (f"Reference Files:\n\n{ref_context}\n\n---\n\n"
                f'Script for Episode "{episode_name}":\n\n{script_text}')
    return f'Script for Episode "{episode_name}":\n\n{script_text}'


def _match_episode_detail(episode_name: str, detail_files: list) -> str:
    """detail_files: [(filename, content), ...]. Match by stripping _episode_detail suffix."""
    ep_lo = episode_name.lower().strip()
    for filename, content in detail_files:
        stem = re.sub(r'_episode_detail(\.md|\.txt)?$', '', filename, flags=re.IGNORECASE).lower().strip()
        if stem == ep_lo:
            return content
    # Fallback: stem prefix overlap
    for filename, content in detail_files:
        stem = re.sub(r'_episode_detail(\.md|\.txt)?$', '', filename, flags=re.IGNORECASE).lower().strip()
        if stem and (stem in ep_lo or ep_lo in stem):
            return content
    return ""


def _process_one_episode(job_dir: Path, script_text: str, episode_name: str, ref_files: dict) -> str:
    """Run full Stage 2 pipeline for one episode. Returns Excel filename."""
    user_msg = _build_stage2_user_msg(ref_files, script_text, episode_name)
    messages = [{"role": "user", "content": user_msg}]

    _log(f"  Running 5-step breakdown for '{episode_name}'...")
    response = call_api_chat(STAGE2_SYSTEM, messages, label=f"breakdown:{episode_name}")
    if not response:
        raise RuntimeError(f"No breakdown response for {episode_name}")

    _log(f"  --- Breakdown summary ({episode_name}) ---")
    for line in response.splitlines()[:8]:
        _log(f"  {line}")
    _log("  ---")

    messages.append({"role": "assistant", "content": response})
    if "BREAKDOWN COMPLETE" not in response:
        _log("  !! Breakdown message not detected — attempting Excel generation anyway")

    messages.append({"role": "user", "content": MAKE_EXCEL_MSG})

    all_rows = []
    batch_num = 1
    max_batches = 20
    while batch_num <= max_batches:
        _log(f"  Fetching batch {batch_num}...")
        response = call_api_chat(STAGE2_SYSTEM, messages, label=f"{episode_name}:batch-{batch_num}")
        if not response:
            _log(f"  x  Empty response on batch {batch_num} — stopping")
            break
        rows = parse_shot_rows(response)
        all_rows.extend(rows)
        _log(f"  Batch {batch_num}: {len(rows)} rows (total: {len(all_rows)})")
        messages.append({"role": "assistant", "content": response})
        if "ALL SHOTS COMPLETE" in response:
            _log(f"  ok  All batches received for {episode_name}")
            break
        messages.append({"role": "user", "content": "NEXT"})
        batch_num += 1

    if not all_rows:
        raise RuntimeError(f"No shot rows parsed for {episode_name}")

    safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', episode_name)
    xlsx_name = f"{safe_name}_breakdown.xlsx"
    write_excel(all_rows, job_dir / xlsx_name, episode_name)
    _log(f"  ok  Excel: {xlsx_name}  ({len(all_rows)} shots)")
    return xlsx_name


def run_stage2_pipeline(job_id: str, job_dir: Path, episodes: list, ref_files_global: dict, detail_files: list):
    """Process multiple episodes sequentially. Each gets its own Excel."""
    log_queue = jobs[job_id]["queue"]
    _set_job_context(job_id, log_queue)

    try:
        _log(f"\n{'=' * 60}")
        _log(f"  STAGE 2 — Shot Breakdown Pipeline")
        _log(f"  Episodes to process: {len(episodes)}")
        for i, ep in enumerate(episodes, 1):
            _log(f"    {i}. {ep['name']}")
        _log(f"{'=' * 60}\n")

        verify_auth()

        jobs[job_id]["progress"]["started_at"] = time.time()
        jobs[job_id]["progress"]["total"]     = len(episodes)

        output_files = []
        failures     = []

        for i, ep in enumerate(episodes, 1):
            ep_name = ep["name"]
            _log(f"\n  ═══ EPISODE {i}/{len(episodes)}: {ep_name} ═══")
            jobs[job_id]["progress"]["stage"] = f"Episode {i}/{len(episodes)}: {ep_name}"

            ref_files = dict(ref_files_global)
            matched = _match_episode_detail(ep_name, detail_files)
            if matched:
                ref_files["episode_detail"] = matched
                _log(f"  Matched episode_detail for {ep_name}")
            elif len(detail_files) == 1:
                ref_files["episode_detail"] = detail_files[0][1]
                _log(f"  Using only available episode_detail ({detail_files[0][0]})")

            try:
                xlsx = _process_one_episode(job_dir, ep["script_text"], ep_name, ref_files)
                output_files.append(xlsx)
                jobs[job_id]["output_files"] = list(output_files)
            except Exception as e:
                _log(f"  [ERROR] Episode {ep_name} failed: {e}")
                failures.append(ep_name)

            jobs[job_id]["progress"]["completed"] = i

        jobs[job_id]["output_files"] = output_files
        jobs[job_id]["output_file"]  = output_files[0] if output_files else None

        _log(f"\n{'=' * 60}")
        _log(f"  STAGE 2 COMPLETE")
        _log(f"  Successful: {len(output_files)}/{len(episodes)}")
        if failures:
            _log(f"  Failed:     {len(failures)} — {', '.join(failures)}")
        _log(f"{'=' * 60}\n")

        jobs[job_id]["status"] = "done"

    except Exception as e:
        _log(f"\n[ERROR] {e}")
        jobs[job_id]["status"] = "failed"
    finally:
        log_queue.put(None)


# ── Flask helpers ─────────────────────────────────────────────────────────────

def _new_job(job_dir: Path) -> tuple:
    job_id    = str(uuid.uuid4())
    log_queue = queue.Queue()
    jobs[job_id] = {
        "status":      "running",
        "queue":       log_queue,
        "job_dir":     job_dir,
        "tokens":      {"input": 0, "output": 0, "calls": 0},
        "tokens_lock": threading.Lock(),
        "output_file": None,
    }
    return job_id, log_queue


def _collect_stage1_files(job_dir: Path) -> list:
    result = []
    for subfolder in ["show level files", "episode details"]:
        d = job_dir / subfolder
        if d.exists():
            for f in sorted(d.glob("*.md")):
                result.append({"name": f.name, "subfolder": subfolder})
    return result


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config")
def api_config():
    return jsonify({"model": ARGUS_MODEL})


# Stage 1 ──

@app.route("/api/stage1/run", methods=["POST"])
def api_stage1_run():
    mode    = request.form.get("mode", "full")
    workers = int(request.form.get("workers", "3") or "3")
    episode = request.form.get("episode", "").strip()
    files   = request.files.getlist("scripts")

    if not ARGUS_API_KEY or not ARGUS_BASE_URL:
        return jsonify({"error": "ARGUS_API_KEY / ARGUS_BASE_URL not set in .env"}), 500
    if not files or all(f.filename == "" for f in files):
        return jsonify({"error": "No script files uploaded"}), 400
    if mode == "episode" and not episode:
        return jsonify({"error": "Episode filename required"}), 400

    job_id  = str(uuid.uuid4())
    job_dir = WORKSPACE / job_id
    scripts_dir = job_dir / "episodic scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        if f.filename:
            f.save(str(scripts_dir / Path(f.filename).name))

    log_queue = queue.Queue()
    jobs[job_id] = {
        "status":       "running",
        "queue":        log_queue,
        "job_dir":      job_dir,
        "tokens":       {"input": 0, "output": 0, "calls": 0},
        "tokens_lock":  threading.Lock(),
        "output_file":  None,
        "output_files": [],
        "progress":     {"total": 0, "completed": 0, "stage": "", "started_at": time.time()},
    }

    threading.Thread(
        target=run_stage1_pipeline,
        args=(job_id, job_dir, mode, workers, episode),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


@app.route("/api/stage1/download/<job_id>/<path:filename>")
def api_stage1_download(job_id: str, filename: str):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job_dir = jobs[job_id]["job_dir"]
    for subfolder in ["show level files", "episode details"]:
        candidate = job_dir / subfolder / filename
        if candidate.exists():
            return send_file(str(candidate), as_attachment=True, download_name=filename)
    return jsonify({"error": "file not found"}), 404


# Stage 2 ──

@app.route("/api/stage2/run", methods=["POST"])
def api_stage2_run():
    if not ARGUS_API_KEY or not ARGUS_BASE_URL:
        return jsonify({"error": "ARGUS_API_KEY / ARGUS_BASE_URL not set in .env"}), 500

    episode_name_input = request.form.get("episode_name", "").strip()
    script_text        = request.form.get("script_text", "").strip()

    try:
        # Build episodes list — one entry per script file, or one from pasted text
        episodes = []
        if script_text:
            episodes.append({
                "name":        episode_name_input or "episode",
                "script_text": script_text,
            })
        else:
            for f in request.files.getlist("script"):
                if not f.filename:
                    continue
                stem = Path(f.filename).stem.strip()
                episodes.append({
                    "name":        stem or "episode",
                    "script_text": _read_upload_as_text(f),
                })

        if not episodes:
            return jsonify({"error": "No script provided"}), 400

        # Shared reference files (concatenated if multiple per type)
        ref_files_global = {}
        for key in ["show_tone_bible", "character_canvas", "location_reference"]:
            parts = [_read_upload_as_text(f) for f in request.files.getlist(f"ref_{key}") if f.filename]
            if parts:
                ref_files_global[key] = "\n\n---\n\n".join(parts)

        # Episode details — keep as list so we can match per episode by filename
        detail_files = [
            (f.filename, _read_upload_as_text(f))
            for f in request.files.getlist("ref_episode_detail") if f.filename
        ]
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 400

    job_id  = str(uuid.uuid4())
    job_dir = WORKSPACE / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    log_queue = queue.Queue()
    jobs[job_id] = {
        "status":       "running",
        "queue":        log_queue,
        "job_dir":      job_dir,
        "tokens":       {"input": 0, "output": 0, "calls": 0},
        "tokens_lock":  threading.Lock(),
        "output_file":  None,
        "output_files": [],
        "progress":     {"total": len(episodes), "completed": 0, "stage": "Starting", "started_at": time.time()},
    }

    threading.Thread(
        target=run_stage2_pipeline,
        args=(job_id, job_dir, episodes, ref_files_global, detail_files),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id, "episode_count": len(episodes)})


@app.route("/api/stage2/download/<job_id>")
def api_stage2_download(job_id: str):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job      = jobs[job_id]
    filename = request.args.get("filename") or job.get("output_file")
    if not filename:
        return jsonify({"error": "no output file yet"}), 404

    path = job["job_dir"] / filename
    if not path.exists():
        return jsonify({"error": "file missing"}), 404
    # Only allow files inside the job dir (prevent path escape)
    try:
        path.resolve().relative_to(job["job_dir"].resolve())
    except ValueError:
        return jsonify({"error": "invalid filename"}), 400
    return send_file(str(path), as_attachment=True, download_name=filename)


# Database ──

@app.route("/api/db/shows")
def api_db_shows():
    try:
        shows = _supabase("GET", "/shows", params={
            "select": "id,name,created_at",
            "order":  "created_at.desc",
        })
        return jsonify(shows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/shows/<show_id>/files")
def api_db_show_files(show_id: str):
    try:
        files = _supabase("GET", "/show_files", params={
            "select":  "id,file_type,filename,content,created_at",
            "show_id": f"eq.{show_id}",
            "order":   "file_type.asc,created_at.asc",
        })
        return jsonify(files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/save", methods=["POST"])
def api_db_save():
    body      = request.get_json(force=True) or {}
    job_id    = body.get("job_id", "")
    show_name = body.get("show_name", "").strip()

    if not show_name:
        return jsonify({"error": "Show name required"}), 400
    if job_id not in jobs:
        return jsonify({"error": "Job not found — job may have expired"}), 404

    job_dir = jobs[job_id]["job_dir"]
    try:
        result  = _supabase("POST", "/shows", {"name": show_name})
        show_id = result[0]["id"]

        saved = 0
        for subfolder in ["show level files", "episode details"]:
            d = job_dir / subfolder
            if d.exists():
                for f in sorted(d.glob("*.md")):
                    _supabase("POST", "/show_files", {
                        "show_id":   show_id,
                        "file_type": _classify_file_type(f.name),
                        "filename":  f.name,
                        "content":   f.read_text(encoding="utf-8"),
                    })
                    saved += 1

        return jsonify({"show_id": show_id, "show_name": show_name, "files_saved": saved})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Shared ──

@app.route("/api/stream/<job_id>")
def api_stream(job_id: str):
    if job_id not in jobs:
        return Response("data: [job not found]\n\nevent: done\ndata:\n\n", mimetype="text/event-stream")
    log_queue = jobs[job_id]["queue"]

    def generate():
        while True:
            try:
                line = log_queue.get(timeout=30)
            except queue.Empty:
                yield "data: [waiting...]\n\n"
                continue
            if line is None:
                yield "event: done\ndata:\n\n"
                break
            yield f"data: {line.replace(chr(10), ' ')}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job   = jobs[job_id]
    files = _collect_stage1_files(job["job_dir"])
    prog = dict(job.get("progress", {}))
    if prog.get("started_at"):
        elapsed = time.time() - prog["started_at"]
        prog["elapsed_sec"] = int(elapsed)
        done  = prog.get("completed", 0)
        total = prog.get("total", 0)
        if done > 0 and total > 0 and elapsed > 0:
            rate    = done / elapsed
            remaining = total - done
            prog["eta_sec"] = int(remaining / rate) if rate > 0 and remaining > 0 else 0
        prog.pop("started_at", None)  # don't leak raw timestamp

    return jsonify({
        "status":       job["status"],
        "files":        files,
        "output_file":  job.get("output_file"),
        "output_files": job.get("output_files", []),
        "progress":     prog,
        "tokens":       job.get("tokens", {"input": 0, "output": 0, "calls": 0}),
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  Shot Generation Pipeline — Stage 1 + Stage 2")
    print(f"  Model : {ARGUS_MODEL or '(not set)'}")
    print("  Open  : http://localhost:5000\n")
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
