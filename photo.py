import sqlite3
from typing import Dict, List, Set
import csv
from pathlib import Path
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

# Configure logging to output to a file in the current directory
# This sets up logging to both a file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_processing.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)

DB_PATH: str = r"file_database.db"
FILES_TABLE: str = "files"
# Path to output CSV file containing all file information        
OUTPUT_CSV: str = r"file_list.csv"
# Path to output CSV file containing duplicate file information
DUPLICATES_CSV: str = r"duplicate_files.csv"
# Output HTML file path
OUTPUT_HTML: str = "duplicate_viewer.html"

def init_database() -> None:
    """
    Initialize the SQLite database with required tables
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create files table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filepath TEXT UNIQUE NOT NULL,
            creation_time TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        )
    ''')

    # Create duplicates table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS duplicates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sha256 TEXT NOT NULL,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            creation_time TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            duplicate_count INTEGER NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    logging.info(f"Database initialized at {DB_PATH}")

def load_existing_file_cache() -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
    """
    Load existing file information from database to avoid reprocessing
        
    Returns:
        Dict[Tuple[str, int], Dict[str, Union[str, int]]]: A dictionary mapping (filepath, file_size) 
        tuples to file metadata dictionaries
    """
    file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]] = {}
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT filename, filepath, creation_time, file_size, sha256 FROM files')
        rows = cursor.fetchall()
        
        for row in rows:
            filename, filepath, creation_time, file_size, sha256 = row
            cache_key: Tuple[str, int] = (filepath, file_size)
            file_cache[cache_key] = {
                'filename': filename,
                'filepath': filepath,
                'creation_time': creation_time,
                'file_size': file_size,
                'sha256': sha256
            }
        
        logging.info(f"Loaded {len(file_cache)} existing file records from database")
    except Exception as e:
        logging.warning(f"Could not load existing data from database {DB_PATH}: {e}")
    
    return file_cache

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
                logging.debug(f"Skipping SHA256 calculation for {filepath} (already processed)")
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

def save_files_to_db(file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
    """
    Save all file information to database
    
    Args:
        file_data_list (List[Optional[Dict[str, Union[str, int]]]]): List of file metadata dictionaries
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute('DELETE FROM files')
    
    # Insert new data
    for file_data in file_data_list:
        if file_data:
            cursor.execute('''
                INSERT OR REPLACE INTO files (filename, filepath, creation_time, file_size, sha256)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                file_data['filename'],
                file_data['filepath'],
                file_data['creation_time'],
                file_data['file_size'],
                file_data['sha256']
            ))
    
    conn.commit()
    conn.close()
    logging.info(f"Saved {len([f for f in file_data_list if f])} file records to database")

def write_all_files_csv(file_data_list: List[Optional[Dict[str, Union[str, int]]]], 
                        output_csv: str) -> None:
    """
    Write all file information to CSV
    
    Args:
        file_data_list (List[Optional[Dict[str, Union[str, int]]]]): List of file metadata dictionaries
        output_csv (str): Path to output CSV file
    """
    # Define CSV column headers
    headers: List[str] = ['filename', 'filepath', 'creation_time', 'file_size', 'sha256']
    
    # Open CSV file for writing
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        # Create CSV writer with specified headers
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        # Write header row
        writer.writeheader()
        
        # Write each file's metadata to CSV
        file_data: Optional[Dict[str, Union[str, int]]]
        for file_data in file_data_list:
            if file_data:
                writer.writerow(file_data)

def save_duplicates_to_db(duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
    """
    Save duplicate files information to database
    
    Args:
        duplicates (Dict[str, List[Dict[str, Union[str, int]]]]): Dictionary of duplicate file groups
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute('DELETE FROM duplicates')
    
    # Insert new data
    for sha256, files in duplicates.items():
        duplicate_count: int = len(files)
        for file_data in files:
            cursor.execute('''
                INSERT INTO duplicates (sha256, filename, filepath, creation_time, file_size, duplicate_count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                sha256,
                file_data['filename'],
                file_data['filepath'],
                file_data['creation_time'],
                file_data['file_size'],
                duplicate_count
            ))
    
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(duplicates)} duplicate groups to database")

def write_duplicates_csv(duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
    """
    Write duplicate files information to CSV
    
    Args:
        duplicates (Dict[str, List[Dict[str, Union[str, int]]]]): Dictionary of duplicate file groups
    """
    # Define CSV column headers including duplicate count
    headers: List[str] = ['sha256', 'filename', 'filepath', 'creation_time', 'file_size', 'duplicate_count']
    
    # Open CSV file for writing
    with open(DUPLICATES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        # Create CSV writer with specified headers
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        # Write header row
        writer.writeheader()
        
        # Write each duplicate file with its group information
        sha256: str
        files: List[Dict[str, Union[str, int]]]
        for sha256, files in duplicates.items():
            # Count number of duplicates in this group
            duplicate_count: int = len(files)
            # Write each file in the duplicate group
            file_data: Dict[str, Union[str, int]]
            for file_data in files:
                # Copy file data and add SHA256 hash and duplicate count
                row: Dict[str, Union[str, int]] = file_data.copy()
                row['sha256'] = sha256
                row['duplicate_count'] = duplicate_count
                writer.writerow(row)

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
    
    # Write all files to CSV
    logging.info(f"Writing all file information to {OUTPUT_CSV}")
    save_files_to_db(file_results)
    
    # Find and write duplicates if requested
    logging.info("Finding duplicate files...")
    duplicates = find_duplicates(file_results)
    logging.info(f"Found {len(duplicates)} groups of duplicate files")
    
    if duplicates:
        logging.info(f"Writing duplicate file information to {DUPLICATES_CSV}")
        save_duplicates_to_db(duplicates)
    else:
        logging.info("No duplicate files found")
    
    return file_results

def generate_html_viewer() -> None:
    """
    Generate an HTML page to view the first 10 groups of duplicate images
    """
    # Read data from database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM duplicates ORDER BY sha256')
    rows = cursor.fetchall()
    
    # Group files by SHA256 hash
    groups = []
    current_group = []
    prev_sha256 = None
    
    for row in rows:
        # Row format: id, sha256, filename, filepath, creation_time, file_size, duplicate_count
        _, sha256, filename, filepath, creation_time, file_size, _ = row
        
        row_dict = {
            'sha256': sha256,
            'filename': filename,
            'filepath': filepath,
            'creation_time': creation_time,
            'file_size': file_size
        }
        
        if sha256 != prev_sha256:
            if current_group:
                groups.append(current_group)
                current_group = []
        current_group.append(row_dict)
        prev_sha256 = sha256
    
    # Don't forget the last group
    if current_group:
        groups.append(current_group)
    
    conn.close()
    
    # Generate HTML
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Duplicate Files Viewer</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .group {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .group-header {
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
            margin-bottom: 15px;
        }
        .group-title {
            font-size: 18px;
            font-weight: bold;
            color: #333;
        }
        .sha256 {
            font-family: monospace;
            font-size: 14px;
            color: #666;
            word-break: break-all;
        }
        .files-container {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
        }
        .file-card {
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            width: 200px;
            background-color: #fafafa;
        }
        .file-image {
            width: 100%;
            height: 150px;
            object-fit: cover;
            border-radius: 4px;
            background-color: #eee;
        }
        .file-info {
            margin-top: 10px;
            font-size: 12px;
        }
        .file-name {
            font-weight: bold;
            margin-bottom: 5px;
            word-break: break-word;
        }
        .file-path {
            color: #666;
            margin-bottom: 5px;
            word-break: break-word;
        }
        .file-time {
            color: #888;
            margin-bottom: 3px;
        }
        .file-size {
            color: #888;
            margin-bottom: 5px;
        }
        h1 {
            color: #333;
        }
        .note {
            background-color: #fff8e1;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 4px;
        }
        .delete-btn {
            background-color: #ff4444;
            color: white;
            border: none;
            padding: 5px 10px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 11px;
            width: 100%;
            margin-top: 8px;
        }
        .delete-btn:hover {
            background-color: #cc0000;
        }
        .deleted {
            opacity: 0.5;
            text-decoration: line-through;
        }
    </style>
</head>
<body>
    <h1>Duplicate Files Viewer</h1>
    
    <div class="note">
        <p><strong>Note:</strong> This page shows the first 10 groups of duplicate files. 
        Images are displayed using file paths - they will only appear if the paths are accessible from this HTML file.</p>
    </div>
    
    <script>
        function deleteFile(filePath, element) {
            // Fix escaped backslashes in the file path
            const cleanPath = filePath.replace(/\\\\/g, '\\\\');
            if (confirm("Are you sure you want to delete this file?\\n" + cleanPath)) {
                // Call Python backend
                fetch('http://localhost:5000/delete-file', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({filePath: cleanPath})
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        element.closest('.file-card').classList.add('deleted');
                        element.disabled = true;
                        element.textContent = 'Deleted';
                    } else {
                        alert('Error deleting file: ' + data.message);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('Failed to connect to backend service');
                });
            }
        }
    </script>
"""

    # Add first 10 groups to HTML
    for i, group in enumerate(groups[:10]):
        sha256 = group[0]['sha256']
        html_content += f"""
    <div class="group">
        <div class="group-header">
            <div class="group-title">Group {i+1} ({len(group)} duplicates)</div>
            <div class="sha256">SHA256: {sha256}</div>
        </div>
        <div class="files-container">
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
            
            # Escape backslashes for JavaScript
            js_safe_path = file_path.replace('\\', '\\\\')
            
            html_content += f"""
            <div class="file-card">
"""
            
            if is_image:
                html_content += f"                <img src=\"{file_path}\" alt=\"{file_name}\" class=\"file-image\" onerror=\"this.style.display='none';\">\n"
            else:
                html_content += f"                <div class=\"file-image\" style=\"display:flex;align-items:center;justify-content:center;background-color:#eee;color:#999;\">No preview</div>\n"
            
            html_content += f"""                <div class="file-info">
                    <div class="file-name">{file_name}</div>
                    <div class="file-path">{file_path}</div>
                    <div class="file-time">Created: {creation_time}</div>
                    <div class="file-size">{size_str}</div>
                    <button class="delete-btn" onclick="deleteFile('{js_safe_path}', this)">Delete File</button>
                </div>
            </div>
"""
        
        html_content += "        </div>\n    </div>\n"

    html_content += """
</body>
</html>
"""
    
    # Write HTML to file
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"HTML viewer generated: {OUTPUT_HTML}")


def find_duplicate_file(directory_paths: List[str] ) -> None:

    
    logging.info("Script started")              
    
    # Generate file information CSV
    process_multiple_directories(directory_paths, OUTPUT_CSV, DUPLICATES_CSV)
    logging.info(f"All file information saved to {OUTPUT_CSV}")
    if DUPLICATES_CSV:
        logging.info(f"Duplicate file information saved to {DUPLICATES_CSV}")
    print(f"All file information saved to {OUTPUT_CSV}")
    if DUPLICATES_CSV:
        print(f"Duplicate file information saved to {DUPLICATES_CSV}")

def refresh_duplicates_db() -> None:
    """
    Refresh the duplicates database by removing entries for files that no longer exist
    and also removing all other entries with the same SHA256 value.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all duplicates grouped by SHA256
    cursor.execute('SELECT DISTINCT sha256 FROM duplicates')
    sha256_list = cursor.fetchall()
    
    valid_sha256 = []
    
    for (sha256,) in sha256_list:
        cursor.execute('SELECT filepath FROM duplicates WHERE sha256 = ?', (sha256,))
        filepaths = cursor.fetchall()
        
        # Check if all files in this group exist
        all_files_exist = True
        for (filepath,) in filepaths:
            if not os.path.exists(filepath):
                all_files_exist = False
                break
        
        # Only keep entries if all files in the group exist
        if all_files_exist:
            valid_sha256.append(sha256)
        # If any file is missing, we drop the entire group
    
    # Delete invalid entries
    deleted_count = 0
    for (sha256,) in sha256_list:
        if sha256 not in valid_sha256:
            cursor.execute('DELETE FROM duplicates WHERE sha256 = ?', (sha256,))
            deleted_count += cursor.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"Refreshed duplicates database. Removed {deleted_count} invalid entries.")

def refresh_duplicates_csv() -> None:
    """
    Refresh the duplicates CSV file by removing entries for files that no longer exist
    and also removing all other entries with the same SHA256 value.
    
    
    """    
    output_csv_path = DUPLICATES_CSV
    
    # First pass: Identify which files still exist
    existing_files_by_sha256: Dict[str, List[dict]] = {}
    missing_sha256_set: Set[str] = set()
    
    with open(DUPLICATES_CSV, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        
        # Group entries by SHA256
        for row in reader:
            sha256 = row['sha256']
            filepath = row['filepath']
            
            if sha256 not in existing_files_by_sha256:
                existing_files_by_sha256[sha256] = []
            
            existing_files_by_sha256[sha256].append(row)
    
    # Check which files exist and identify completely missing groups
    valid_entries = []
    for sha256, entries in existing_files_by_sha256.items():
        # Check if all files in this group exist
        all_files_exist = True
        for entry in entries:
            if not os.path.exists(entry['filepath']):
                all_files_exist = False
                break
        
        # Only keep entries if all files in the group exist
        if all_files_exist:
            valid_entries.extend(entries)
        # If any file is missing, we drop the entire group (including existing files)
    
    # Write back the filtered entries
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(valid_entries)
    
    print(f"Refreshed {DUPLICATES_CSV}. Kept {len(valid_entries)} entries.")

# You can call this function in your main code like:



# Example usage
if __name__ == "__main__":
    # Specify your directory paths (can be multiple)
    directory_paths: List[str] = [
        r"F:\\photo",
        r"G:\\视频",  # Add more directories as needed
        # r"D:\Documents"
    ]
    # find_duplicate_file(directory_paths)
    refresh_duplicates_db()
    # Generate the HTML viewer
    generate_html_viewer()
