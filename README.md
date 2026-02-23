# Audio File Deduplication

Identifies and moves duplicate audio files across a **reference folder** (your protected originals) and a **source folder** (the collection to clean).

## How It Works — 3-Stage Tiered Pipeline

Deduplication runs in three stages to minimise disk I/O. Each stage eliminates non-matches using the least work possible, so expensive full hashing is only performed on genuine candidates.

| Stage | Method | Disk cost |
|-------|--------|-----------|
| 1 | **Size filter** — files with unique sizes cannot be identical | Zero (metadata only) |
| 2 | **Partial hash** — hash only the first 4 KB of each size-matched file | Minimal I/O |
| 3 | **Full SHA-256 hash** — fully hash all Stage-2 survivors | Only for true candidates |

**Key design decision**: both folders are pooled together in Stage 1. Processing each folder independently (as naive implementations do) would miss cross-folder duplicates whenever a reference file is the sole occupant of its size bucket — it would never be hashed and source copies would go undetected.

A source file is flagged as a duplicate when:
- Its full hash matches any reference file → **moved to output folder**
- Its full hash matches another source file with no reference match → first occurrence kept, **rest moved**

## Requirements

- Python 3.8+
- `tqdm`

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Basic
python audio_deduplication.py \
  --reference /path/to/originals \
  --source    /path/to/scan \
  --output    /path/to/duplicates

# Dry run — log what would happen, move nothing
python audio_deduplication.py \
  --reference /path/to/originals \
  --source    /path/to/scan \
  --output    /path/to/duplicates \
  --dry-run

# All options
python audio_deduplication.py --help
```

## CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--reference` | *(required)* | Protected reference folder (originals) |
| `--source` | *(required)* | Source folder to deduplicate |
| `--output` | *(required)* | Folder to move duplicates into |
| `--log-file` | `audio_deduplication.log` | Log file path |
| `--log-level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `--threads` | `8` | Parallel worker count |
| `--hash-algorithm` | `sha256` | Hash algorithm (`sha256`, `md5`, …) |
| `--partial-hash-size` | `4096` | Bytes to read in Stage 2 partial hash |
| `--max-hash-bytes` | *(full file)* | Cap Stage 3 hash at N bytes; `0` = full file |
| `--dry-run` | `False` | Simulate; do not move any files |
| `--retries` | `3` | Move retry attempts on failure |
| `--retry-delay` | `1.0` | Initial retry delay in seconds (exponential backoff) |

## Audio Extensions

`.mp3` `.flac` `.wav` `.aac` `.ogg` `.aif` `.aiff`

Edit `AUDIO_EXTENSIONS` at the top of the script to customise.

## Running Tests

```bash
pip install pytest
pytest test_deduplication.py -v
```

Test coverage includes:
- `compute_hash` — correct digest, partial-read boundary conditions, file smaller than cap, missing file
- `find_audio_files` — extension matching, recursion, case-insensitivity, directory exclusion
- `find_duplicates` — **regression guard for the critical cross-folder bug**, internal duplicates, multiple copies, no false positives, empty inputs, same-size-different-content
- `move_file` — dry run, basic move, single and multiple collision resolution, nested directory creation, content preservation

## Caution

This script **moves** files from the source folder. Always verify your intended actions with `--dry-run` before operating on real data, and keep backups.
