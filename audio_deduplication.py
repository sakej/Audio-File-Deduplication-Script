import os
import hashlib
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ======= USER CONFIGURATION =======
REFERENCE_FOLDER = Path(r"")  # Protected directory (original files)
SOURCE_FOLDER = Path(r"")  # Directory to scan for duplicates
OUTPUT_FOLDER = Path(r"")  # Where to move duplicate files
LOG_FILE = Path(r"audio_deduplication.log")
LOGGING_LEVEL = logging.INFO         # DEBUG, INFO, WARNING, ERROR
THREADS = 8                          # Number of concurrent workers
BLOCK_SIZE = 65536                   # Block size for reading files (64 KB)
HASH_ALGORITHM = "md5"               # Hashing algorithm to use
MAX_HASH_BYTES = 10 * 1024 * 1024    # Hash only first 10MB for speed
DRY_RUN = False                      # Set to True for dry run mode
RETRIES = 3                          # Number of retry attempts for file moves
RETRY_DELAY = 1                      # Initial retry delay in seconds (exponential backoff)
AUDIO_EXTENSIONS = [".mp3", ".flac", ".wav", ".aac", ".ogg", ".aif", ".aiff"]
# ==================================

class UnicodeFormatter(logging.Formatter):
    """Custom formatter to handle Unicode characters in log messages"""
    def format(self, record):
        return super().format(record).encode('utf-8', errors='replace').decode('utf-8')

# Configure logging
logger = logging.getLogger()
logger.setLevel(LOGGING_LEVEL)

# Create formatter and handlers
formatter = UnicodeFormatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
console_handler = logging.StreamHandler()

# Add formatters to handlers
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Clear existing handlers and add new ones
logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def get_file_size(file_path):
    """Get the size of a file in bytes with error handling"""
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        logger.error(f"Error getting size of {file_path}: {str(e)}")
        return None

def get_file_hash(file_path):
    """
    Generate a hash for a file using specified algorithm and configuration
    Hashes only the first MAX_HASH_BYTES of the file for performance
    """
    try:
        hasher = hashlib.new(HASH_ALGORITHM)
        bytes_read = 0
        
        with open(file_path, 'rb') as f:
            while bytes_read < MAX_HASH_BYTES:
                chunk = f.read(min(BLOCK_SIZE, MAX_HASH_BYTES - bytes_read))
                if not chunk:
                    break
                hasher.update(chunk)
                bytes_read += len(chunk)
        
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Error hashing {file_path}: {str(e)}")
        return None

def find_audio_files(root_dir):
    """Recursively find audio files with target extensions showing progress"""
    audio_files = []
    all_files = list(Path(root_dir).rglob('*'))
    
    for file_path in tqdm(all_files, desc="Scanning directory"):
        if file_path.suffix.lower() in AUDIO_EXTENSIONS:
            audio_files.append(str(file_path))
    
    logger.info(f"Found {len(audio_files)} audio files in {root_dir}")
    return audio_files

def process_files(files):
    """Threaded processing of audio files with progress tracking"""
    size_map = {}
    hash_map = {}
    
    # Step 1: Collect file sizes with progress
    logger.info("Collecting file sizes...")
    for file in tqdm(files, desc="Checking file sizes"):
        size = get_file_size(file)
        if size is not None:
            size_map.setdefault(size, []).append(file)

    # Step 2: Compute hashes only for files with matching sizes
    logger.info("Computing hashes...")
    files_to_hash = [f for size_group in size_map.values() 
                    if len(size_group) > 1 for f in size_group]
    
    logger.info(f"Hashing {len(files_to_hash)} potential duplicates")
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(get_file_hash, f): f for f in files_to_hash}
        for future in tqdm(as_completed(futures), total=len(futures), 
                         desc="Processing files"):
            file_path = futures[future]
            file_hash = future.result()
            if file_hash:
                hash_map.setdefault(file_hash, []).append(file_path)
    
    return size_map, hash_map

def move_file(source, destination_folder):
    """
    Move a file with retries and cross-filesystem support
    Handles duplicate filenames and dry run mode
    """
    source_path = Path(source)
    dest_path = Path(destination_folder) / source_path.name
    
    try:
        # Generate unique filename if needed
        counter = 1
        while dest_path.exists():
            dest_path = dest_path.with_name(
                f"{source_path.stem}_{counter}{source_path.suffix}"
            )
            counter += 1

        # Handle dry run mode
        if DRY_RUN:
            logger.info(f"[Dry Run] Would move: {source} -> {dest_path}")
            return

        # Ensure destination directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Attempt move with retries
        attempt = 0
        while attempt <= RETRIES:
            try:
                source_path.rename(dest_path)  # Try fast rename first
                logger.info(f"Moved: {source} -> {dest_path}")
                return
            except OSError:
                # Fallback to shutil.move for cross-filesystem moves
                shutil.move(str(source_path), str(dest_path))
                logger.info(f"Moved (cross-fs): {source} -> {dest_path}")
                return
            except Exception as e:
                attempt += 1
                if attempt > RETRIES:
                    raise
                delay = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Retry {attempt}/{RETRIES} in {delay}s: {source}")
                time.sleep(delay)

    except Exception as e:
        logger.error(f"Failed to move {source}: {str(e)}")

def move_files_in_parallel(files, destination_folder):
    """Move multiple files in parallel with progress tracking"""
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for file_path in files:
            futures.append(executor.submit(move_file, file_path, destination_folder))

        # Track progress with tqdm
        for future in tqdm(as_completed(futures), total=len(futures), 
                         desc="Moving duplicates"):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Move failed: {str(e)}")

def main():
    """Main deduplication workflow with validation and progress tracking"""
    logger.info("Starting audio file deduplication process")
    
    # Validate folder paths
    if not REFERENCE_FOLDER.exists():
        logger.error(f"Reference folder missing: {REFERENCE_FOLDER}")
        return
    
    if not SOURCE_FOLDER.exists():
        logger.error(f"Source folder missing: {SOURCE_FOLDER}")
        return

    # Prepare output directory
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output folder: {OUTPUT_FOLDER}")
    if DRY_RUN:
        logger.info("DRY RUN MODE ENABLED - No files will be moved")

    # File discovery with progress
    logger.info("Scanning reference folder...")
    reference_files = find_audio_files(REFERENCE_FOLDER)

    logger.info("Scanning source folder...")
    source_files = find_audio_files(SOURCE_FOLDER)

    # Processing pipelines
    logger.info("Processing reference files...")
    _, reference_hashes = process_files(reference_files)

    logger.info("Processing source files...")
    _, source_hashes = process_files(source_files)

    # Duplicate identification
    logger.info("Identifying duplicates...")
    duplicates = []
    reference_set = set(reference_hashes.keys())
    
    for file_hash, paths in source_hashes.items():
        if file_hash in reference_set:
            duplicates.extend(paths)
        elif len(paths) > 1:
            duplicates.extend(paths[1:])  # Keep first instance in source

    logger.info(f"Identified {len(duplicates)} duplicate files")

    # File moving phase
    if duplicates:
        logger.info("Initiating file move process...")
        move_files_in_parallel(duplicates, OUTPUT_FOLDER)
    else:
        logger.info("No duplicates found to move")

    logger.info("Deduplication process completed")

if __name__ == "__main__":
    main()
