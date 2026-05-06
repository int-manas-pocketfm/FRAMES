# Stage 1 — Reference File Generation
## Instruction Manual

Stage 1 reads your raw episode scripts and produces all the reference files the rest of the pipeline depends on. Nothing downstream works correctly without these files — they are the single source of truth for every character, location, outfit, and visual tone across all episodes.

---

## What Stage 1 Produces

Running Stage 1 creates two categories of files:

### Show-Level Files (3 files — generated once per show)

These are permanent reference documents that apply across every episode. They live in the `show level files/` folder.

**1. Show Tone Bible**
The visual DNA of the show. Defines:
- Color palettes — named palettes tied to emotional states, characters, or story phases (e.g. "Nara's Warmth Palette", "Gwen's Cold Authority Palette")
- Visual motifs — recurring visual elements and how to render them
- Emotional registers — the distinct visual modes the show operates in (e.g. "Quiet Tension", "Public Humiliation", "Private Joy")
- Cinematography notes — shot distance tendencies, angle conventions, depth of field style
- Style Anchor — the fixed line that opens every Shot Detail cell in Stage 2

Stage 2 uses the Show Tone Bible to fill Part 3 of every Shot Detail row (Color Palette, Visual Motif, Emotional Register). Without it, every shot gets generic tone values that don't match the show.

**2. Character Canvas**
Complete visual profiles for every named character in the show. For each character:
- Full physical description (build, skin, hair, eyes, face, distinguishing marks)
- Default outfit with exact detail (garment type, color, material, fit, every accessory with position and size)
- Recurring outfit variations (training gear, formal wear, sleepwear, power outfit, etc.)
- Key visual tells — 3–5 shortcuts that make the character instantly recognizable in a close-up
- Arc note — how the character's appearance evolves across the show

Stage 2 uses the Character Canvas to write consistent outfit descriptions in every Shot Description and Shot Detail row. Without it, Gemini reinvents the character's appearance in every image.

**3. Location Reference**
Full visual profiles for every location in the show. For each location:
- Physical description (size, architecture, condition, key features, furniture)
- Lighting profile by time of day (day/night, light source, color temperature)
- Atmosphere and default mood
- Dramatic usage — what kinds of scenes happen here, how the space changes
- Image generation notes — wide shot framing, recurring close-up elements, consistency requirements

Stage 2 uses the Location Reference to write the location block in Part 2 of every Shot Detail row. Without it, Gemini generates a different-looking version of the same room in every shot.

---

### Episode Detail Files (1 file per episode)

One file per episode, stored in the `episode details/` folder. Named `ep001_episode_detail.md`, `ep002_episode_detail.md`, etc.

Each Episode Detail file covers:

**Summary** — 2–4 sentences: what happens, emotional core, how it ends.

**Characters** — every named character who appears, with their outfit for this specific episode (which may differ from their default outfit in the Character Canvas).

**Locations** — every location used in this episode with lighting and atmosphere.

**Props** — visually prominent or narratively significant props only.

**Key Visual Moments** — 5–10 most distinct and emotionally significant scenes, described as visual moments suitable for image generation.

**Tone Arc** — the episode's emotional journey in three beats: opening tone, mid-episode shift, closing tone.

**Continuity Flags** — anything that carries over from previous episodes or must be tracked for future ones (outfit changes, injuries, location state changes).

**Shot Description Notes** — production notes for the image generation stage (visual effects needing physical translation, close-up requirements, crowd framing notes, recurring motifs).

Stage 2 uses the Episode Detail file to cross-reference outfits and locations before writing every Shot Description. It is the episode-specific authority — it overrides the general Character Canvas if a character is wearing something different this episode.

---

## How to Run Stage 1

### Folder setup

Before running, your show folder must have this structure:

```
GDA/
└── episodic scripts/
    ├── ep001.md
    ├── ep002.md
    └── ep003.md
```

Scripts must be `.md` files. Drop every episode script into `episodic scripts/` before running.

### Commands

```bash
# Generate everything — all episode detail files + all 3 show-level files
python run.py --folder "C:/Shows/GDA" --stage 1 --full

# Generate show-level files only (Tone Bible, Character Canvas, Location Reference)
python run.py --folder "C:/Shows/GDA" --stage 1 --show-level

# Generate all episode detail files only
python run.py --folder "C:/Shows/GDA" --stage 1 --all-episodes

# Generate episode detail file for one specific episode
python run.py --folder "C:/Shows/GDA" --stage 1 --episode ep001.md
```

### Output locations

After running `--full`, your show folder will look like:

```
GDA/
├── episodic scripts/          ← your scripts (unchanged)
├── show level files/          ← 3 show-level files created here
│   ├── show_tone_bible.md
│   ├── character_canvas.md
│   └── location_reference.md
└── episode details/           ← one file per episode created here
    ├── ep001_episode_detail.md
    ├── ep002_episode_detail.md
    └── ep003_episode_detail.md
```

---

## What to Review After Stage 1

Before running Stage 2, review the generated files and check:

### Show Tone Bible
- Are the color palettes grounded in the actual show? They should be named after specific characters or story phases, not generic descriptions.
- Are the visual motifs pulled from the scripts, not invented?
- Are the emotional registers specific enough to be useful? "Sadness" is too generic. "Quiet resignation after a public loss" is correct.

### Character Canvas
- Is every named character documented?
- Are the outfits specific enough to recreate? "Blue dress" is wrong. "Floor-length cobalt silk dress with off-shoulder neckline, fitted bodice, no jewelry" is correct.
- Are distinguishing marks (scars, birthmarks, recurring accessories) described with exact position, size, shape, and color?

### Location Reference
- Is every location documented, including ones that appear only once?
- Does the lighting profile match how the location actually appears in the scripts?

### Episode Detail Files
- Do the outfit descriptions in each episode file match what actually happens in that episode? Characters sometimes change clothes mid-episode — check that both outfits are captured.
- Are continuity flags complete? Any outfit changes, injuries, or prop states that carry over to the next episode must be flagged.
- Are the Key Visual Moments described visually, not narratively? "She realizes her mistake" is narrative. "She stops mid-step, hand still on the door handle, eyes dropping to the floor" is visual.

---

## Rules the AI Follows When Generating These Files

**For all files:**
- Every element must be grounded in the scripts — nothing is invented
- All descriptions are physical and visual — never abstract or emotional interpretation in description fields
- Inferred details (things not explicitly stated in the scripts) are marked *(inferred)*
- Output only the markdown document — no preamble, no commentary

**For show-level files specifically:**
- Show Tone Bible: palettes and motifs must be named and specific — never generic color words
- Character Canvas: outfits are described at the level of a costume supervisor — every garment, color, material, fit, and accessory
- Location Reference: lighting is described by time of day with exact light source, quality, and color temperature — never vague

**For episode detail files:**
- Outfits in the Characters table override the Character Canvas for that specific episode
- Every character who appears on screen is documented — even briefly seen characters
- Props included only if they are visually prominent or carry story significance

---

## When to Re-Run Stage 1

- **New episode scripts added:** Run `--all-episodes` to generate detail files for new episodes. Run `--show-level` if enough new episodes were added to meaningfully change the show's visual world.
- **Script changed after initial run:** Re-run `--episode ep_N.md` for the specific episode that changed.
- **Show-level files look wrong or incomplete:** Re-run `--show-level`. Stage 1 always regenerates — it never skips existing files.
- **Starting a new show:** Run `--full` to generate everything from scratch.

---

## Model and Speed

Stage 1 uses `claude-sonnet-4-6` by default. Each API call generates one file. For a 48-episode show with `--full`, that is 51 calls (48 episode detail files + 3 show-level files) running sequentially. Expect approximately 2–4 minutes per file, so a full run on a large show can take 2–3 hours.

If you only need show-level files (e.g. you already have episode detail files), use `--show-level` — that is just 3 calls.

---

## File Naming Convention

| File | Name in output folder |
|---|---|
| Show Tone Bible | `show_tone_bible.md` |
| Character Canvas | `character_canvas.md` |
| Location Reference | `location_reference.md` |
| Episode Detail (Ep 1) | `ep001_episode_detail.md` |
| Episode Detail (Ep 12) | `ep012_episode_detail.md` |

The episode stem comes directly from the script filename. If your script is named `Ch_01_Episode_Title.md`, the detail file will be `Ch_01_Episode_Title_episode_detail.md`. Keep script filenames clean and consistent.
