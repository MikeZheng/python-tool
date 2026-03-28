import json
from pathlib import Path
import argparse
import time
import os
import hashlib
from datetime import datetime
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys
from collections import defaultdict
from typing import List, Dict, Tuple, Optional, Any, Union

# Import storage modules
from sqlite_storage import SQLiteStorage
from storage_base import StorageInterface

# Configure logging to output to a file in the current directory
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_processing.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Global storage instance
storage: Optional[StorageInterface] = None
# Output HTML file path
OUTPUT_HTML: str = "duplicate_viewer.html"


def get_storage() -> StorageInterface:
    """Get SQLite storage instance"""
    global storage
    if storage is None:
        storage = SQLiteStorage()
    return storage


def load_existing_file_cache() -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
    """Load existing file information to avoid reprocessing"""
    global storage
    return storage.load_existing_file_cache()


def calculate_sha256(file_path: str) -> Optional[str]:
    """
    Calculate SHA256 hash of a file

    Args:
        file_path (str): Path to the file to hash

    Returns:
        Optional[str]: SHA256 hash as hexadecimal string, or None if an error occurs
    """
    # Initialize SHA256 hasher
    sha256_hash = hashlib.sha256()
    try:
        # Open file in binary mode
        with open(file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        # Return the hexadecimal representation of the hash
        return sha256_hash.hexdigest()
    except Exception as e:
        # Return None if there's an error reading the file
        return None


def process_single_file_with_cache(file_info: Tuple[str, str],
                                 file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]]) -> Optional[Dict[str, Union[str, int]]]:
    """
    Process a single file and return its information, using cache to skip if possible

    Args:
        file_info (Tuple[str, str]): Tuple containing (file_path, root_directory)
        file_cache (Dict[Tuple[str, int], Dict[str, Union[str, int]]]): Cache of previously processed files

    Returns:
        Optional[Dict[str, Union[str, int]]]: Dictionary containing file metadata, or None if processing fails
    """
    # Extract file path and root directory from tuple
    file_path: str
    root: str
    file_path, root = file_info

    try:
        # Get file statistics (size, timestamps, etc.)
        stat_info = os.stat(file_path)

        # Extract filename from full path
        filename: str = os.path.basename(file_path)

        # Full file path
        filepath: str = file_path

        # Format creation time as human-readable string
        creation_time: str = datetime.fromtimestamp(stat_info.st_ctime).strftime('%Y-%m-%d %H:%M:%S')

        # Get file size in bytes
        file_size: int = stat_info.st_size

        # Create cache key using filepath and file size for lookup
        cache_key: Tuple[str, int] = (filepath, file_size)

        # Check if file already exists in cache
        if cache_key in file_cache:
            # Retrieve cached entry
            cached_entry: Dict[str, Union[str, int]] = file_cache[cache_key]
            # Return cached data if it has a valid SHA256
            if cached_entry.get('sha256'):
                logging.info(f"Skipping SHA256 calculation for {filepath} (already processed)")
                return cached_entry

        # Calculate SHA256 if not in cache or size changed
        sha256: Optional[str] = calculate_sha256(file_path)

        # If SHA256 calculation was successful, return file metadata
        if sha256:
            return {
                'filename': filename,
                'filepath': filepath,
                'creation_time': creation_time,
                'file_size': file_size,
                'sha256': sha256
            }
    except Exception as e:
        # Log error if file processing fails
        logging.error(f"Error processing file {file_path}: {e}")

    # Return None if processing failed
    return None


def collect_files_from_directories(directory_paths: List[str]) -> List[Tuple[str, str]]:
    """
    Collect all files from multiple directories

    Args:
        directory_paths (List[str]): List of directory paths to scan

    Returns:
        List[Tuple[str, str]]: List of tuples containing (file_path, root_directory)
    """
    # Initialize list to store file information
    files_to_process: List[Tuple[str, str]] = []

    # Iterate through each directory path
    for directory_path in directory_paths:
        # Check if directory exists
        if not os.path.exists(directory_path):
            # Log warning and skip if directory doesn't exist
            logging.warning(f"Directory does not exist: {directory_path}")
            continue

        # Log directory scanning progress
        logging.info(f"Scanning directory: {directory_path}")

        # Walk through directory tree
        for root, dirs, files in os.walk(directory_path):
            # Process each file in the directory
            for file in files:
                # Construct full file path
                file_path: str = os.path.join(root, file)
                # Add file information to processing list
                files_to_process.append((file_path, root))

    return files_to_process


def find_duplicates(file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> Dict[str, List[Dict[str, Union[str, int]]]]:
    """
    Find duplicate files based on SHA256 hash

    Args:
        file_data_list (List[Optional[Dict[str, Union[str, int]]]]): List of file metadata dictionaries

    Returns:
        Dict[str, List[Dict[str, Union[str, int]]]]: Dictionary mapping SHA256 hashes to lists of file metadata
    """
    # Initialize defaultdict to group files by SHA256 hash
    sha256_groups: Dict[str, List[Dict[str, Union[str, int]]]] = defaultdict(list)

    # Group files by SHA256 hash
    file_data: Optional[Dict[str, Union[str, int]]]
    for file_data in file_data_list:
        # Check if file data exists and contains SHA256 hash
        if file_data and 'sha256' in file_data:
            # Group file by its SHA256 hash
            sha256_groups[file_data['sha256']].append(file_data)

    # Filter groups with more than one file (duplicates)
    duplicates: Dict[str, List[Dict[str, Union[str, int]]]] = {
        sha256: files for sha256, files in sha256_groups.items() if len(files) > 1
    }

    return duplicates


def process_multiple_directories(directory_paths: List[str],
                               max_workers: Optional[int] = None) -> List[Optional[Dict[str, Union[str, int]]]]:
    """
    Process multiple directories and generate file information with duplicate detection

    Args:
        directory_paths (List[str]): List of directory paths to process
        max_workers (Optional[int]): Maximum number of worker processes to use

    Returns:
        List[Optional[Dict[str, Union[str, int]]]]: List of processed file metadata
    """
    global storage
    # Log start of processing
    logging.info(f"Starting to process {len(directory_paths)} directories: {directory_paths}")

    # Load existing file cache to avoid reprocessing
    file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]] = load_existing_file_cache()

    # Collect all files from all directories
    logging.info("Collecting files from all directories...")
    files_to_process: List[Tuple[str, str]] = collect_files_from_directories(directory_paths)
    total_files: int = len(files_to_process)
    logging.info(f"Found {total_files} files to process")

    # Return early if no files found
    if total_files == 0:
        logging.warning("No files found to process")
        return []

    # Determine number of worker processes based on CPU cores if not specified
    if max_workers is None:
        max_workers = min(32, (mp.cpu_count() or 1) + 4)

    # Initialize counters and results list
    processed_count: int = 0
    successful_count: int = 0
    file_results: List[Optional[Dict[str, Union[str, int]]]] = []
    skipped_count: int = 0

    # Process files in parallel with status monitoring
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with cache information
        future_to_file: Dict[Any, str] = {
            executor.submit(process_single_file_with_cache, file_info, file_cache): file_info[0]
            for file_info in files_to_process
        }

        # Log start of parallel processing
        logging.info(f"Started processing with {max_workers} workers")

        # Process completed tasks as they finish
        start_time: float = time.time()
        last_status_time: float = start_time

        future: Any
        for future in as_completed(future_to_file):
            processed_count += 1
            file_path: str = future_to_file[future]

            try:
                # Get result from completed task
                result: Optional[Dict[str, Union[str, int]]] = future.result()
                if result:
                    file_results.append(result)
                    # Check if this was a cached result
                    cache_key: Tuple[str, int] = (result['filepath'], result['file_size'])
                    if cache_key in file_cache and file_cache[cache_key].get('sha256') == result['sha256']:
                        skipped_count += 1
                    successful_count += 1
            except Exception as e:
                # Log error if task failed
                logging.error(f"Error getting result for {file_path}: {e}")

            # Provide regular status updates
            current_time: float = time.time()
            if (processed_count % max(1, total_files // 50) == 0 or
                current_time - last_status_time >= 30 or  # Every 30 seconds
                processed_count == total_files):

                # Calculate processing speed
                elapsed_time: float = current_time - start_time
                files_per_second: float = processed_count / elapsed_time if elapsed_time > 0 else 0

                # Log progress information
                logging.info(f"Progress: {processed_count}/{total_files} files "
                           f"({successful_count} successful, {skipped_count} skipped, "
                           f"{files_per_second:.1f} files/sec, "
                           f"{max_workers} workers active)")
                last_status_time = current_time

    # Log completion summary
    logging.info(f"Completed processing. Total files processed: {successful_count}/{total_files} "
               f"({skipped_count} files skipped due to caching)")

    # Write all files to database
    logging.info(f"Writing all file information")
    storage.save_files(file_results)
    return file_results


def generate_html_viewer() -> None:
    """
    Generate an HTML page to view duplicate files
    """
    groups = storage.get_duplicate_groups()

    # Generate HTML
    html_content = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Duplicate Files Viewer</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 font-sans mx-auto px-4 py-5 max-w-7xl">
    <h1 class="text-3xl font-bold text-gray-800 mb-6">Duplicate Files Viewer</h1>

    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-4 mb-6 rounded">
        <div class="flex">
            <div class="flex-shrink-0">
                <svg class="h-5 w-5 text-yellow-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
                </svg>
            </div>
            <div class="ml-3">
                <p class="text-sm text-yellow-700">
                    <strong>Note:</strong> This page shows duplicate files. Please use the main application for full functionality.
                </p>
            </div>
        </div>
    </div>

    <div id="content">
"""

    # Add groups to HTML
    for i, group in enumerate(groups):
        sha256 = group[0]['sha256']
        html_content += f"""
    <div class="bg-white rounded-lg p-5 mb-6 shadow">
        <div class="border-b-2 border-gray-200 pb-3 mb-4">
            <div class="text-xl font-bold text-gray-800">Group {i+1} ({len(group)} duplicates)</div>
            <div class="font-mono text-sm text-gray-600 break-all">SHA256: {sha256}</div>
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
"""

        for file_info in group:
            file_path = file_info['filepath']
            file_name = file_info['filename']
            file_size = int(file_info['file_size'])
            creation_time = file_info.get('creation_time', 'Unknown')

            # Format file size
            if file_size < 1024:
                size_str = f"{file_size} bytes"
            elif file_size < 1024*1024:
                size_str = f"{file_size//1024} KB"
            else:
                size_str = f"{file_size//(1024*1024)} MB"

            # Try to determine if it's an image based on extension
            ext = Path(file_name).suffix.lower()
            is_image = ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff']

            html_content += f"""
            <div class="border border-gray-300 rounded p-3 bg-gray-50">
"""

            if is_image:
                html_content += f"                <img src=\"{file_path}\" alt=\"{file_name}\" class=\"w-full h-32 object-cover rounded\" onerror=\"this.style.display='none';\">\n"
            else:
                html_content += f"                <div class=\"w-full h-32 flex items-center justify-center bg-gray-200 rounded text-gray-500\">No preview</div>\n"

            html_content += f"""                <div class="mt-3 text-xs">
                    <div class="font-bold mb-1 truncate\">{file_name}</div>
                    <div class="text-gray-600 mb-1 truncate\">{file_path}</div>
                    <div class="text-gray-500 mb-0.5\">Created: {creation_time}</div>
                    <div class="text-gray-500 mb-1\">{size_str}</div>
                </div>
            </div>
"""

        html_content += "        </div>\n    </div>\n"

    html_content += """
    </div>

    <script>
        function formatFileSize(bytes) {
            if (bytes < 1024) {
                return bytes + ' B';
            } else if (bytes < 1024 * 1024) {
                return (bytes / 1024).toFixed(2) + ' KB';
            } else {
                return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
            }
        }
    </script>
</body>
</html>
"""

    # Write HTML to file
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)

    logging.info(f"HTML viewer generated: {OUTPUT_HTML}")


# API function for app.py
def scan_directories_api(directory_paths: List[str]) -> Dict[str, Union[bool, str, int]]:
    """
    API function to scan directories and return results

    Args:
        directory_paths: List of directory paths to scan

    Returns:
        Dict with success status, message, and files processed count
    """
    try:
        global storage
        # Initialize storage (SQLite only now)
        storage = get_storage()

        # Process directories
        results = process_multiple_directories(directory_paths)

        return {
            'success': True,
            'message': 'Directory scanned successfully',
            'files_processed': len([r for r in results if r is not None])
        }
    except Exception as e:
        return {
            'success': False,
            'message': f'Error scanning directories: {str(e)}',
            'files_processed': 0
        }


def main():
    """
    CLI entry point
    """
    # Scan directories
    # python photo.py --directories "F:\\photo" "G:\\video"
    """
    parser = argparse.ArgumentParser(description='Find duplicate files')
    parser.add_argument('--directories', nargs='+', required=False,
                       help='Directories to scan for duplicates')
    parser.add_argument('--refresh', action='store_true',
                       help='Refresh database by removing entries for non-existent files')
    parser.add_argument('--generate-html', action='store_true',
                       help='Generate HTML viewer')

    args = parser.parse_args()

    # Initialize storage (SQLite only)
    global storage
    storage = get_storage()

    # Handle refresh operation
    if args.refresh:
        storage.refresh_duplicates()

    # Process directories
    directory_paths = args.directories
    if directory_paths:
        process_multiple_directories(directory_paths)

    # Generate HTML viewer if requested
    if args.generate_html:
        generate_html_viewer()


if __name__ == "__main__":
    main()
