"""
audio_deduplication.py

Identifies and moves duplicate audio files across a reference folder (originals,
protected) and a source folder (the collection to clean).

Deduplication uses a 3-stage tiered pipeline to minimise disk I/O:
  Stage 1 — Size filter  : files with unique sizes cannot be identical (zero I/O)
  Stage 2 — Partial hash : hash only the first PARTIAL_HASH_SIZE bytes (cheap I/O)
  Stage 3 — Full hash    : full hash of candidates surviving both prior stages

A source file is flagged as a duplicate when:
  - Its full hash matches any reference file's full hash, OR
  - Its full hash matches another source file's full hash (internal duplicate);
    the first occurrence is kept and the rest are moved.

Usage:
  python audio_deduplication.py --reference /originals --source /toscan --output /dupes
  python audio_deduplication.py --help
"""

import os
import hashlib
import logging
import shutil
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from tqdm import tqdm
from pathlib import Path

# ======= DEFAULT CONFIGURATION =======
# All values below can be overridden via CLI arguments (run with --help).
REFERENCE_FOLDER   = Path(r"")         # Protected directory (original files)
SOURCE_FOLDER      = Path(r"")         # Directory to scan for duplicates
OUTPUT_FOLDER      = Path(r"")         # Where to move duplicate files
LOG_FILE           = Path("audio_deduplication.log")
LOGGING_LEVEL      = "INFO"            # DEBUG | INFO | WARNING | ERROR
THREADS            = 8                 # Parallel worker count
HASH_ALGORITHM     = "sha256"          # Hashing algorithm (sha256 recommended)
PARTIAL_HASH_SIZE  = 4096             # Bytes to read for Stage 2 (4 KB)
MAX_HASH_BYTES     = None             # None = full file; int = byte cap for Stage 3
DRY_RUN            = False            # True = simulate without moving files
RETRIES            = 3               # Move retry attempts
RETRY_DELAY        = 1.0             # Initial retry delay in seconds (exponential backoff)
AUDIO_EXTENSIONS   = {".mp3", ".flac", ".wav", ".aac", ".ogg", ".aif", ".aiff"}
# =====================================

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class UnicodeFormatter(logging.Formatter):
    """Ensure Unicode characters survive logging on all platforms."""

    def format(self, record: logging.LogRecord) -> str:
        return super().format(record).encode("utf-8", errors="replace").decode("utf-8")


def setup_logging(log_file: Path, level: str) -> None:
    """Initialise the root logger with console + file handlers."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)
    root.handlers.clear()

    formatter = UnicodeFormatter("%(asctime)s - %(levelname)s - %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    root.addHandler(fh)
    root.addHandler(ch)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments; defaults come from the DEFAULT CONFIGURATION block."""
    p = argparse.ArgumentParser(
        description="Audio file deduplication — reference folder vs source folder.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--reference",         type=Path,  default=REFERENCE_FOLDER,  help="Protected reference folder (originals)")
    p.add_argument("--source",            type=Path,  default=SOURCE_FOLDER,     help="Source folder to deduplicate")
    p.add_argument("--output",            type=Path,  default=OUTPUT_FOLDER,     help="Folder to move duplicates into")
    p.add_argument("--log-file",          type=Path,  default=LOG_FILE,          help="Log file path")
    p.add_argument("--log-level",                     default=LOGGING_LEVEL,     help="Logging verbosity")
    p.add_argument("--threads",           type=int,   default=THREADS,           help="Parallel worker count")
    p.add_argument("--hash-algorithm",               default=HASH_ALGORITHM,    help="Hash algorithm (sha256, md5 ...)")
    p.add_argument("--partial-hash-size", type=int,   default=PARTIAL_HASH_SIZE, help="Bytes for Stage 2 partial hash")
    p.add_argument("--max-hash-bytes",    type=int,   default=MAX_HASH_BYTES,    help="Cap Stage 3 full hash at N bytes (0 = full file)")
    p.add_argument("--dry-run",           action="store_true", default=DRY_RUN,  help="Simulate; do not move files")
    p.add_argument("--retries",           type=int,   default=RETRIES,           help="Move retry attempts")
    p.add_argument("--retry-delay",       type=float, default=RETRY_DELAY,       help="Initial retry delay in seconds")
    return p.parse_args()


# ---------------------------------------------------------------------------
# File utilities
# ---------------------------------------------------------------------------

def get_file_size(file_path: str) -> Optional[int]:
    """Return file size in bytes, or None on error."""
    try:
        return os.path.getsize(file_path)
    except Exception as exc:
        logger.error("Error getting size of %s: %s", file_path, exc)
        return None


def compute_hash(file_path: str, algorithm: str, max_bytes: Optional[int]) -> Optional[str]:
    """
    Hash *file_path* with *algorithm*.

    When *max_bytes* is None or 0 the entire file is read.
    When *max_bytes* > 0 only the first *max_bytes* bytes are read.
    I/O is performed in 64 KB blocks regardless.
    """
    BLOCK = 65536
    try:
        hasher = hashlib.new(algorithm)
        bytes_read = 0
        with open(file_path, "rb") as fh:
            while True:
                if max_bytes:
                    remaining = max_bytes - bytes_read
                    if remaining <= 0:
                        break
                    to_read = min(BLOCK, remaining)
                else:
                    to_read = BLOCK
                chunk = fh.read(to_read)
                if not chunk:
                    break
                hasher.update(chunk)
                bytes_read += len(chunk)
        return hasher.hexdigest()
    except Exception as exc:
        logger.error("Error hashing %s: %s", file_path, exc)
        return None


def find_audio_files(root_dir: Path) -> List[str]:
    """Recursively collect audio files under *root_dir*."""
    results: List[str] = []
    all_paths = list(root_dir.rglob("*"))
    for path in tqdm(all_paths, desc=f"Scanning {root_dir.name}", unit="file"):
        if path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS:
            results.append(str(path))
    logger.info("Found %d audio files in %s", len(results), root_dir)
    return results


# ---------------------------------------------------------------------------
# Core deduplication — 3-stage tiered pipeline
# ---------------------------------------------------------------------------

def _hash_pool(
    files: List[str],
    algorithm: str,
    max_bytes: Optional[int],
    threads: int,
    desc: str,
) -> Dict[str, str]:
    """Hash *files* in parallel; return {path: hexdigest} for successful hashes."""
    results: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_path = {
            executor.submit(compute_hash, f, algorithm, max_bytes): f for f in files
        }
        for future in tqdm(
            as_completed(future_to_path),
            total=len(future_to_path),
            desc=desc,
            unit="file",
        ):
            path = future_to_path[future]
            digest = future.result()
            if digest is not None:
                results[path] = digest
    return results


def find_duplicates(
    reference_files: List[str],
    source_files: List[str],
    algorithm: str,
    partial_hash_size: int,
    max_hash_bytes: Optional[int],
    threads: int,
) -> List[str]:
    """
    3-stage tiered deduplication across a reference set and a source set.

    Stage 1 — Size filter  : pool both sets together; any file whose size is
                             unique across the entire combined pool cannot be a
                             duplicate and is eliminated (zero disk I/O).
    Stage 2 — Partial hash : hash the first *partial_hash_size* bytes of every
                             Stage-1 survivor; eliminate files whose partial hash
                             is unique (cheap I/O).
    Stage 3 — Full hash    : fully hash every Stage-2 survivor; determine actual
                             duplicates by comparing final digests.

    NOTE — pooling both folders in Stage 1 is intentional and critical:
    if the folders were processed separately, a reference file that happens to be
    the only file of its size in the reference folder would never be hashed and
    source copies of it would go undetected.

    Returns a list of *source* file paths that should be moved.
    """
    reference_set = set(reference_files)
    # Reference files first so that in internal-source-duplicate groups the
    # reference path sorts to the front (though we filter by reference_set, not index).
    all_files = reference_files + source_files

    # ── Stage 1: size filter ─────────────────────────────────────────────────
    logger.info("Stage 1 — size filter across %d combined files ...", len(all_files))
    size_map: Dict[int, List[str]] = {}
    for f in tqdm(all_files, desc="Stage 1: size filter", unit="file"):
        size = get_file_size(f)
        if size is not None:
            size_map.setdefault(size, []).append(f)

    stage1_candidates = [
        f for group in size_map.values() if len(group) > 1 for f in group
    ]
    logger.info(
        "Stage 1 — %d / %d files survive (size collision across both folders)",
        len(stage1_candidates),
        len(all_files),
    )
    if not stage1_candidates:
        logger.info("No candidates after Stage 1 — no duplicates.")
        return []

    # ── Stage 2: partial hash ────────────────────────────────────────────────
    logger.info("Stage 2 — partial hash (first %d bytes) ...", partial_hash_size)
    partial_digests = _hash_pool(
        stage1_candidates, algorithm, partial_hash_size, threads,
        "Stage 2: partial hash",
    )

    partial_map: Dict[str, List[str]] = {}
    for path, digest in partial_digests.items():
        partial_map.setdefault(digest, []).append(path)

    stage2_candidates = [
        f for group in partial_map.values() if len(group) > 1 for f in group
    ]
    logger.info(
        "Stage 2 — %d / %d files survive (partial hash collision)",
        len(stage2_candidates),
        len(stage1_candidates),
    )
    if not stage2_candidates:
        logger.info("No candidates after Stage 2 — no duplicates.")
        return []

    # ── Stage 3: full hash ───────────────────────────────────────────────────
    logger.info("Stage 3 — full hash of %d candidates ...", len(stage2_candidates))
    full_digests = _hash_pool(
        stage2_candidates, algorithm, max_hash_bytes, threads,
        "Stage 3: full hash",
    )

    full_map: Dict[str, List[str]] = {}
    for path, digest in full_digests.items():
        full_map.setdefault(digest, []).append(path)

    # ── Duplicate identification ─────────────────────────────────────────────
    duplicates: List[str] = []
    for digest, paths in full_map.items():
        if len(paths) < 2:
            continue  # unique full hash — definitely not a duplicate

        ref_paths = [p for p in paths if p in reference_set]
        src_paths = [p for p in paths if p not in reference_set]

        if ref_paths:
            # Every source copy is a duplicate of at least one reference file.
            duplicates.extend(src_paths)
        elif len(src_paths) > 1:
            # Internal source duplicates with no reference match.
            # Keep the first occurrence; move all subsequent copies.
            duplicates.extend(src_paths[1:])

    logger.info("Identified %d duplicate file(s).", len(duplicates))
    return duplicates


# ---------------------------------------------------------------------------
# File moving
# ---------------------------------------------------------------------------

def move_file(
    source: str,
    destination_folder: Path,
    dry_run: bool,
    retries: int,
    retry_delay: float,
) -> None:
    """
    Move *source* into *destination_folder*.

    - Generates a unique filename when a name collision exists in the destination.
    - Tries a fast same-filesystem rename first; falls back to shutil.move for
      cross-filesystem operations (catches the OSError errno EXDEV).
    - Both rename and shutil.move failures are retried up to *retries* times
      with exponential backoff.  The outer for-loop ensures shutil.move failures
      are retried too (original code bypassed retries for the shutil path).
    """
    source_path = Path(source)
    dest_path = destination_folder / source_path.name

    # Resolve filename collision before touching the filesystem.
    counter = 1
    while dest_path.exists():
        dest_path = dest_path.with_name(
            f"{source_path.stem}_{counter}{source_path.suffix}"
        )
        counter += 1

    if dry_run:
        logger.info("[Dry Run] Would move: %s -> %s", source, dest_path)
        return

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):          # attempt 0 = first try, 1..retries = retries
        try:
            try:
                source_path.rename(dest_path)   # fast path: same filesystem (atomic)
            except OSError:
                # Fallback for cross-filesystem moves (errno EXDEV and similar).
                shutil.move(str(source_path), str(dest_path))
            logger.info("Moved: %s -> %s", source, dest_path)
            return                              # success — stop immediately
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    "Retry %d/%d in %.1fs for %s: %s",
                    attempt + 1, retries, delay, source, exc,
                )
                time.sleep(delay)
            # If attempt == retries: loop ends naturally, error logged below.

    logger.error("Failed to move %s after %d retries: %s", source, retries, last_error)


def move_files_in_parallel(
    files: List[str],
    destination_folder: Path,
    dry_run: bool,
    retries: int,
    retry_delay: float,
    threads: int,
) -> None:
    """Move *files* to *destination_folder* in parallel with progress tracking."""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_path = {
            executor.submit(
                move_file, f, destination_folder, dry_run, retries, retry_delay
            ): f
            for f in files
        }
        for future in tqdm(
            as_completed(future_to_path),
            total=len(future_to_path),
            desc="Moving duplicates",
            unit="file",
        ):
            try:
                future.result()
            except Exception as exc:
                logger.error("Move task raised unhandled exception: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    setup_logging(args.log_file, args.log_level)

    logger.info("=== Audio File Deduplication ===")
    logger.info("Reference  : %s", args.reference)
    logger.info("Source     : %s", args.source)
    logger.info("Output     : %s", args.output)
    logger.info(
        "Algorithm  : %s | partial=%dB | full=%s",
        args.hash_algorithm.upper(),
        args.partial_hash_size,
        f"cap {args.max_hash_bytes}B" if args.max_hash_bytes else "full file",
    )
    logger.info("Threads    : %d | Dry run: %s", args.threads, args.dry_run)

    # Validate that required paths were actually provided.
    for flag, val in (("--reference", args.reference), ("--source", args.source), ("--output", args.output)):
        if not val or not str(val).strip():
            logger.error("%s path is required.", flag)
            return

    if not args.reference.exists():
        logger.error("Reference folder not found: %s", args.reference)
        return
    if not args.source.exists():
        logger.error("Source folder not found: %s", args.source)
        return

    args.output.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        logger.info("DRY RUN MODE — no files will be moved.")

    logger.info("Scanning reference folder ...")
    reference_files = find_audio_files(args.reference)

    logger.info("Scanning source folder ...")
    source_files = find_audio_files(args.source)

    if not reference_files:
        logger.warning("Reference folder contains no audio files — nothing to compare against.")
        return
    if not source_files:
        logger.warning("Source folder contains no audio files.")
        return

    duplicates = find_duplicates(
        reference_files=reference_files,
        source_files=source_files,
        algorithm=args.hash_algorithm,
        partial_hash_size=args.partial_hash_size,
        max_hash_bytes=args.max_hash_bytes if args.max_hash_bytes else None,
        threads=args.threads,
    )

    if duplicates:
        logger.info("Moving %d duplicate(s) to %s ...", len(duplicates), args.output)
        move_files_in_parallel(
            duplicates,
            args.output,
            args.dry_run,
            args.retries,
            args.retry_delay,
            args.threads,
        )
    else:
        logger.info("No duplicates found — source folder is clean.")

    logger.info("=== Deduplication complete ===")


if __name__ == "__main__":
    main()
