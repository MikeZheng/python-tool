import time
import os
import hashlib
import csv
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

def load_existing_file_cache(output_csv: str) -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
    """
    Load existing file information from CSV to avoid reprocessing
    
    Args:
        output_csv (str): Path to the CSV file containing previously processed file information
        
    Returns:
        Dict[Tuple[str, int], Dict[str, Union[str, int]]]: A dictionary mapping (filepath, file_size) 
        tuples to file metadata dictionaries
    """
    # Initialize empty cache dictionary
    file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]] = {}
    
    # Check if the CSV file exists
    if os.path.exists(output_csv):
        try:
            # Open and read the existing CSV file
            with open(output_csv, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Iterate through each row in the CSV
                for row in reader:
                    # Ensure required columns exist in the row
                    if all(key in row for key in ['filepath', 'file_size', 'sha256']):
                        try:
                            # Create a cache key using filepath and file size for quick lookup
                            cache_key: Tuple[str, int] = (row['filepath'], int(row['file_size']))
                            # Store file metadata in the cache
                            file_cache[cache_key] = {
                                'filename': row['filename'],
                                'filepath': row['filepath'],
                                'creation_time': row['creation_time'],
                                'file_size': int(row['file_size']),
                                'sha256': row['sha256']
                            }
                        except (ValueError, KeyError):
                            # Skip rows with invalid data
                            continue
            # Log the number of records loaded
            logging.info(f"Loaded {len(file_cache)} existing file records from {output_csv}")
        except Exception as e:
            # Log warning if CSV file cannot be read
            logging.warning(f"Could not load existing CSV file {output_csv}: {e}")
    else:
        # Log message if no existing CSV file is found
        logging.info("No existing CSV file found, will process all files")
    
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

def write_all_files_csv(file_data_list: List[Optional[Dict[str, Union[str, int]]]], output_csv: str) -> None:
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

def write_duplicates_csv(duplicates: Dict[str, List[Dict[str, Union[str, int]]]], duplicates_csv: str) -> None:
    """
    Write duplicate files information to CSV
    
    Args:
        duplicates (Dict[str, List[Dict[str, Union[str, int]]]]): Dictionary of duplicate file groups
        duplicates_csv (str): Path to output CSV file for duplicates
    """
    # Define CSV column headers including duplicate count
    headers: List[str] = ['sha256', 'filename', 'filepath', 'creation_time', 'file_size', 'duplicate_count']
    
    # Open CSV file for writing
    with open(duplicates_csv, 'w', newline='', encoding='utf-8') as csvfile:
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
                               output_csv: str, 
                               duplicates_csv: Optional[str] = None, 
                               max_workers: Optional[int] = None) -> List[Optional[Dict[str, Union[str, int]]]]:
    """
    Process multiple directories and generate file information with duplicate detection
    
    Args:
        directory_paths (List[str]): List of directory paths to process
        output_csv (str): Path to output CSV file containing all file information
        duplicates_csv (Optional[str]): Path to output CSV file containing duplicate file information
        max_workers (Optional[int]): Maximum number of worker processes to use
        
    Returns:
        List[Optional[Dict[str, Union[str, int]]]]: List of processed file metadata
    """
    # Log start of processing
    logging.info(f"Starting to process {len(directory_paths)} directories: {directory_paths}")
    
    # Load existing file cache to avoid reprocessing
    file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]] = load_existing_file_cache(output_csv)
    
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
    logging.info(f"Writing all file information to {output_csv}")
    write_all_files_csv(file_results, output_csv)
    
    # Find and write duplicates if requested
    if duplicates_csv:
        logging.info("Finding duplicate files...")
        duplicates = find_duplicates(file_results)
        logging.info(f"Found {len(duplicates)} groups of duplicate files")
        
        if duplicates:
            logging.info(f"Writing duplicate file information to {duplicates_csv}")
            write_duplicates_csv(duplicates, duplicates_csv)
        else:
            logging.info("No duplicate files found")
    
    return file_results

# Example usage
if __name__ == "__main__":
    # Specify your directory paths (can be multiple)
    directory_paths: List[str] = [
        r"G:\视频",
        r"G:\照片",  # Add more directories as needed
        # r"D:\Documents"
    ]
    
    # Specify output CSV file paths
    output_csv: str = r"G:\all_files.csv"
    duplicates_csv: Optional[str] = r"G:\duplicate_files.csv"  # Set to None if you don't want duplicates file
    
    logging.info("Script started")
    
    # Generate file information CSV
    process_multiple_directories(directory_paths, output_csv, duplicates_csv)
    logging.info(f"All file information saved to {output_csv}")
    if duplicates_csv:
        logging.info(f"Duplicate file information saved to {duplicates_csv}")
    print(f"All file information saved to {output_csv}")
    if duplicates_csv:
        print(f"Duplicate file information saved to {duplicates_csv}")