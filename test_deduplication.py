"""
test_deduplication.py

Unit tests for audio_deduplication.py

Run with:
    pip install pytest
    pytest test_deduplication.py -v

Coverage:
  - compute_hash      : correct digest, partial reads, file-smaller-than-cap, missing file
  - find_audio_files  : extensions, recursion, case-insensitivity, empty directory
  - find_duplicates   : critical cross-folder regression, internal duplicates,
                        multiple copies, no false positives, empty inputs
  - move_file         : dry run, basic move, collision resolution, directory creation
"""

import hashlib
from pathlib import Path

import pytest

# The module no longer runs side-effects at import time (logging is deferred to
# setup_logging() which is only called from main()), so this import is safe.
from audio_deduplication import (
    compute_hash,
    find_audio_files,
    find_duplicates,
    move_file,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

# Distinct byte sequences with known sizes — keep them large enough to exceed
# PARTIAL_HASH_SIZE (4096 bytes) so stages 2 and 3 use genuinely different reads.
SAMPLE_A = b"audio_alpha_" * 400    # 4800 bytes
SAMPLE_B = b"audio_beta__" * 400    # 4800 bytes — same size, different content
SAMPLE_C = b"audio_alpha_" * 400    # 4800 bytes — byte-for-byte identical to SAMPLE_A
SAMPLE_D = b"audio_delta_" * 200    # 2400 bytes — unique size

assert SAMPLE_A == SAMPLE_C, "SAMPLE_C must be identical to SAMPLE_A"
assert SAMPLE_A != SAMPLE_B, "SAMPLE_A and SAMPLE_B must differ"
assert len(SAMPLE_A) == len(SAMPLE_B), "SAMPLE_A and SAMPLE_B must have the same size"
assert len(SAMPLE_A) != len(SAMPLE_D), "SAMPLE_D must have a distinct size"


def make_file(directory: Path, name: str, content: bytes) -> Path:
    """Create *directory* if necessary, write *content* to *name*, return the path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(content)
    return path


# ---------------------------------------------------------------------------
# compute_hash
# ---------------------------------------------------------------------------

class TestComputeHash:

    def test_full_hash_known_value(self, tmp_path):
        f = make_file(tmp_path, "a.flac", b"hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert compute_hash(str(f), "sha256", None) == expected

    def test_partial_hash_reads_exactly_n_bytes(self, tmp_path):
        content = b"ABCDEFGHIJ" * 200          # 2000 bytes
        f = make_file(tmp_path, "a.mp3", content)
        expected = hashlib.sha256(content[:10]).hexdigest()
        assert compute_hash(str(f), "sha256", 10) == expected

    def test_partial_hash_larger_than_file_hashes_whole_file(self, tmp_path):
        """When max_bytes exceeds file size, the entire file must be hashed."""
        content = b"tiny"
        f = make_file(tmp_path, "a.wav", content)
        expected = hashlib.sha256(content).hexdigest()
        assert compute_hash(str(f), "sha256", 999_999) == expected

    def test_max_bytes_none_hashes_full_file(self, tmp_path):
        content = SAMPLE_A
        f = make_file(tmp_path, "a.ogg", content)
        expected = hashlib.sha256(content).hexdigest()
        assert compute_hash(str(f), "sha256", None) == expected

    def test_max_bytes_zero_hashes_full_file(self, tmp_path):
        """max_bytes=0 is treated as 'no cap' (same as None)."""
        content = SAMPLE_A
        f = make_file(tmp_path, "a.flac", content)
        expected = hashlib.sha256(content).hexdigest()
        assert compute_hash(str(f), "sha256", 0) == expected

    def test_md5_algorithm(self, tmp_path):
        f = make_file(tmp_path, "a.aac", b"test")
        expected = hashlib.md5(b"test").hexdigest()
        assert compute_hash(str(f), "md5", None) == expected

    def test_missing_file_returns_none(self, tmp_path):
        assert compute_hash(str(tmp_path / "ghost.mp3"), "sha256", None) is None

    def test_different_content_different_hash(self, tmp_path):
        f1 = make_file(tmp_path, "a.mp3", SAMPLE_A)
        f2 = make_file(tmp_path, "b.mp3", SAMPLE_B)
        assert compute_hash(str(f1), "sha256", None) != compute_hash(str(f2), "sha256", None)

    def test_identical_content_same_hash(self, tmp_path):
        f1 = make_file(tmp_path, "a.mp3", SAMPLE_A)
        f2 = make_file(tmp_path, "c.mp3", SAMPLE_C)   # identical bytes
        assert compute_hash(str(f1), "sha256", None) == compute_hash(str(f2), "sha256", None)


# ---------------------------------------------------------------------------
# find_audio_files
# ---------------------------------------------------------------------------

class TestFindAudioFiles:

    def test_finds_known_extensions(self, tmp_path):
        make_file(tmp_path, "track.mp3",  b"x")
        make_file(tmp_path, "track.flac", b"x")
        make_file(tmp_path, "image.jpg",  b"x")   # must be ignored
        make_file(tmp_path, "notes.txt",  b"x")   # must be ignored
        found = {Path(f).name for f in find_audio_files(tmp_path)}
        assert "track.mp3"  in found
        assert "track.flac" in found
        assert "image.jpg"  not in found
        assert "notes.txt"  not in found

    def test_recursive_scan(self, tmp_path):
        sub = tmp_path / "deep" / "nested"
        make_file(sub, "nested.wav", b"x")
        found = find_audio_files(tmp_path)
        assert any("nested.wav" in f for f in found)

    def test_empty_directory_returns_empty_list(self, tmp_path):
        assert find_audio_files(tmp_path) == []

    def test_case_insensitive_extension(self, tmp_path):
        make_file(tmp_path, "UPPER.MP3",  b"x")
        make_file(tmp_path, "mixed.FLaC", b"x")
        assert len(find_audio_files(tmp_path)) == 2

    def test_directories_not_included(self, tmp_path):
        """Subdirectory names that look like audio files must not be returned."""
        fake = tmp_path / "not_a_file.mp3"
        fake.mkdir()
        found = find_audio_files(tmp_path)
        assert not any("not_a_file.mp3" in f for f in found)


# ---------------------------------------------------------------------------
# find_duplicates
# ---------------------------------------------------------------------------

class TestFindDuplicates:
    """These tests cover the 3-stage deduplication logic, including a regression
    guard for the critical cross-folder comparison bug in the original code."""

    def _run(self, ref_files, src_files):
        return find_duplicates(
            reference_files=[str(f) for f in ref_files],
            source_files=[str(f) for f in src_files],
            algorithm="sha256",
            partial_hash_size=4096,
            max_hash_bytes=None,
            threads=2,
        )

    # ── REGRESSION GUARD — the critical original bug ─────────────────────────

    def test_cross_folder_duplicate_detected(self, tmp_path):
        """
        REGRESSION GUARD for the critical original bug.

        Scenario: the reference folder has one file with a size that is unique
        within the reference folder.  The source folder has an exact copy.

        Original behaviour (BUGGY): process_files() was called separately for
        each folder.  The size-filter only kept files whose size appeared more
        than once *within the same folder*.  A reference file that was the sole
        occupant of its size bucket was never hashed, so the cross-folder match
        was never found and the source duplicate was not moved.

        Expected behaviour (FIXED): both folders are pooled together in Stage 1.
        The size group now contains both the reference file and the source copy
        (two files), so both proceed to Stage 2 and Stage 3, the hash match is
        found, and the source copy is correctly returned as a duplicate.
        """
        ref_dir = tmp_path / "ref"
        src_dir = tmp_path / "src"

        ref_a = make_file(ref_dir, "original.mp3", SAMPLE_A)
        src_b = make_file(src_dir, "duplicate.mp3", SAMPLE_C)  # identical to SAMPLE_A

        duplicates = self._run([ref_a], [src_b])

        assert str(src_b) in duplicates, (
            "Source file with content identical to a reference file must be detected "
            "as a duplicate (cross-folder match)"
        )

    def test_reference_file_never_in_result(self, tmp_path):
        """Reference files must never be returned as duplicates."""
        ref_dir = tmp_path / "ref"
        src_dir = tmp_path / "src"
        ref_a = make_file(ref_dir, "original.mp3", SAMPLE_A)
        src_b = make_file(src_dir, "copy.mp3",     SAMPLE_C)

        duplicates = self._run([ref_a], [src_b])
        assert str(ref_a) not in duplicates

    # ── Different files are not flagged ──────────────────────────────────────

    def test_different_content_not_flagged(self, tmp_path):
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        ref_a = make_file(ref_dir, "a.mp3", SAMPLE_A)
        src_b = make_file(src_dir, "b.mp3", SAMPLE_B)  # same size, different content
        assert self._run([ref_a], [src_b]) == []

    def test_different_size_not_flagged(self, tmp_path):
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        ref_a = make_file(ref_dir, "a.mp3", SAMPLE_A)
        src_d = make_file(src_dir, "d.mp3", SAMPLE_D)  # different size entirely
        assert self._run([ref_a], [src_d]) == []

    # ── Internal source duplicates ────────────────────────────────────────────

    def test_internal_source_duplicate_keeps_first(self, tmp_path):
        """Two identical source files with no reference match: keep first, move second."""
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        make_file(ref_dir, "ref.mp3", SAMPLE_D)   # different size — no match with src files
        src1 = make_file(src_dir, "copy1.mp3", SAMPLE_A)
        src2 = make_file(src_dir, "copy2.mp3", SAMPLE_C)  # identical to src1

        duplicates = self._run([ref_dir / "ref.mp3"], [src1, src2])
        assert len(duplicates) == 1
        assert str(src1) not in duplicates, "First occurrence must be kept"
        assert str(src2) in duplicates

    def test_three_internal_duplicates_keeps_first(self, tmp_path):
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        make_file(ref_dir, "ref.mp3", SAMPLE_D)
        s1 = make_file(src_dir, "s1.mp3", SAMPLE_A)
        s2 = make_file(src_dir, "s2.mp3", SAMPLE_C)
        s3 = make_file(src_dir, "s3.mp3", SAMPLE_C)

        duplicates = self._run([ref_dir / "ref.mp3"], [s1, s2, s3])
        assert len(duplicates) == 2
        assert str(s1) not in duplicates

    # ── Multiple source copies of the same reference file ─────────────────────

    def test_multiple_source_copies_all_flagged(self, tmp_path):
        """Three source copies of a reference file — all three must be returned."""
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        ref = make_file(ref_dir, "original.mp3", SAMPLE_A)
        s1  = make_file(src_dir, "copy1.mp3",    SAMPLE_C)
        s2  = make_file(src_dir, "copy2.mp3",    SAMPLE_C)
        s3  = make_file(src_dir, "copy3.mp3",    SAMPLE_C)

        duplicates = self._run([ref], [s1, s2, s3])
        assert len(duplicates) == 3
        for s in [s1, s2, s3]:
            assert str(s) in duplicates

    # ── Edge cases: empty inputs ──────────────────────────────────────────────

    def test_empty_source_returns_empty(self, tmp_path):
        ref_dir = tmp_path / "ref"
        ref = make_file(ref_dir, "a.mp3", SAMPLE_A)
        assert self._run([ref], []) == []

    def test_empty_reference_returns_empty(self, tmp_path):
        """With no reference files there can be no cross-folder duplicates."""
        src_dir = tmp_path / "src"
        src = make_file(src_dir, "a.mp3", SAMPLE_A)
        # One source file alone cannot be a duplicate of anything.
        assert self._run([], [src]) == []

    def test_both_empty_returns_empty(self, tmp_path):
        assert self._run([], []) == []

    # ── Same-size but different content (partial hash separates them) ─────────

    def test_same_size_different_content_not_flagged(self, tmp_path):
        """SAMPLE_A and SAMPLE_B are the same size but differ in content."""
        ref_dir = tmp_path / "ref"; src_dir = tmp_path / "src"
        ref = make_file(ref_dir, "a.mp3", SAMPLE_A)
        src = make_file(src_dir, "b.mp3", SAMPLE_B)
        assert self._run([ref], [src]) == []


# ---------------------------------------------------------------------------
# move_file
# ---------------------------------------------------------------------------

class TestMoveFile:

    def test_dry_run_leaves_source_intact(self, tmp_path):
        src  = make_file(tmp_path / "src", "a.mp3", b"data")
        dest = tmp_path / "dest"
        move_file(str(src), dest, dry_run=True, retries=0, retry_delay=0)
        assert src.exists(), "Dry run must not delete or move the source file"
        assert not dest.exists(), "Dry run must not create the destination directory"

    def test_basic_move_succeeds(self, tmp_path):
        src  = make_file(tmp_path / "src", "track.mp3", b"audio")
        dest = tmp_path / "dest"
        move_file(str(src), dest, dry_run=False, retries=0, retry_delay=0)
        assert not src.exists(), "Source must be removed after move"
        assert (dest / "track.mp3").read_bytes() == b"audio"

    def test_collision_generates_unique_name(self, tmp_path):
        src      = make_file(tmp_path / "src",  "track.mp3", b"new")
        dest_dir = tmp_path / "dest"; dest_dir.mkdir()
        (dest_dir / "track.mp3").write_bytes(b"existing")   # pre-occupy name
        move_file(str(src), dest_dir, dry_run=False, retries=0, retry_delay=0)
        assert (dest_dir / "track_1.mp3").exists(), "Renamed file must exist"
        assert (dest_dir / "track.mp3").read_bytes() == b"existing", "Original must be untouched"

    def test_multiple_collisions_increment_counter(self, tmp_path):
        src      = make_file(tmp_path / "src", "t.mp3", b"new")
        dest_dir = tmp_path / "dest"; dest_dir.mkdir()
        (dest_dir / "t.mp3").write_bytes(b"0")
        (dest_dir / "t_1.mp3").write_bytes(b"1")
        move_file(str(src), dest_dir, dry_run=False, retries=0, retry_delay=0)
        assert (dest_dir / "t_2.mp3").exists()

    def test_move_creates_nested_destination(self, tmp_path):
        src      = make_file(tmp_path / "src", "track.flac", b"flac")
        dest_dir = tmp_path / "a" / "b" / "c"   # entirely non-existent
        move_file(str(src), dest_dir, dry_run=False, retries=0, retry_delay=0)
        assert (dest_dir / "track.flac").exists()

    def test_content_preserved_after_move(self, tmp_path):
        payload = b"binary content " * 500
        src  = make_file(tmp_path / "src", "file.wav", payload)
        dest = tmp_path / "dest"
        move_file(str(src), dest, dry_run=False, retries=0, retry_delay=0)
        assert (dest / "file.wav").read_bytes() == payload
