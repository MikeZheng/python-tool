import csv
import logging
import os
import sys
from typing import Dict, List, Tuple, Optional, Union
from storage_base import StorageInterface

# Constants
OUTPUT_CSV: str = r"file_list.csv"
DUPLICATES_CSV: str = r"duplicate_files.csv"

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

class CSVStorage(StorageInterface):
    """CSV-based storage implementation"""
    
    def load_existing_file_cache(self) -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
        """Load existing file information from CSV to avoid reprocessing"""
        file_cache: Dict[Tuple[str, int], Dict[str, Union[str, int]]] = {}
        
        if os.path.exists(OUTPUT_CSV):
            try:
                logging.info(f"Loading existing file cache from {OUTPUT_CSV}")
                with open(OUTPUT_CSV, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    count = 0
                    for row in reader:
                        if all(key in row for key in ['filepath', 'file_size', 'sha256']):
                            try:
                                cache_key: Tuple[str, int] = (row['filepath'], int(row['file_size']))
                                file_cache[cache_key] = {
                                    'filename': row['filename'],
                                    'filepath': row['filepath'],
                                    'creation_time': row['creation_time'],
                                    'file_size': int(row['file_size']),
                                    'sha256': row['sha256']
                                }
                                count += 1
                            except (ValueError, KeyError):
                                continue
                    logging.info(f"Loaded {count} entries from CSV cache")
            except Exception as e:
                logging.warning(f"Could not load existing CSV file {OUTPUT_CSV}: {e}")
        
        return file_cache
    
    def save_files(self, file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
        """Write all file information to CSV"""
        headers: List[str] = ['filename', 'filepath', 'creation_time', 'file_size', 'sha256']
        
        logging.info(f"Saving {len([f for f in file_data_list if f])} files to {OUTPUT_CSV}")
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for file_data in file_data_list:
                if file_data:
                    writer.writerow(file_data)
        logging.info("File data saved successfully")

    def save_duplicates(self, duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
        """Write duplicate files information to CSV"""
        headers: List[str] = ['sha256', 'filename', 'filepath', 'creation_time', 'file_size', 'duplicate_count']
        
        total_duplicates = sum(len(files) for files in duplicates.values())
        logging.info(f"Saving {total_duplicates} duplicate entries to {DUPLICATES_CSV}")
        
        with open(DUPLICATES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for sha256, files in duplicates.items():
                duplicate_count: int = len(files)
                for file_data in files:
                    row: Dict[str, Union[str, int]] = file_data.copy()
                    row['sha256'] = sha256
                    row['duplicate_count'] = duplicate_count
                    writer.writerow(row)
        logging.info("Duplicate files saved successfully")

    def refresh_duplicates(self) -> None:
        """Refresh the duplicates CSV file by removing entries for files that no longer exist"""
        if not os.path.exists(DUPLICATES_CSV):
            logging.info("No duplicates CSV file found, skipping refresh")
            return
            
        logging.info("Refreshing duplicates CSV file")
        # First pass: Identify which files still exist
        existing_files_by_sha256: Dict[str, List[dict]] = {}
        
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
        removed_count = 0
        for sha256, entries in existing_files_by_sha256.items():
            # Check if all files in this group exist
            all_files_exist = True
            for entry in entries:
                if not os.path.exists(entry['filepath']):
                    all_files_exist = False
                    logging.debug(f"File no longer exists: {entry['filepath']}")
                    break
            
            # Only keep entries if all files in the group exist
            if all_files_exist:
                valid_entries.extend(entries)
            else:
                removed_count += len(entries)
        
        # Write back the filtered entries
        with open(DUPLICATES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(valid_entries)
        
        logging.info(f"Refreshed duplicates CSV. Removed {removed_count} invalid entries, kept {len(valid_entries)} valid entries")

    def get_duplicate_groups(self, limit: Optional[int] = None) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups from CSV for HTML viewer
        
        Args:
            limit (Optional[int]): Maximum number of duplicate groups to return. 
                                If None, returns all groups.
        """
        groups = []
        current_group = []
        prev_sha256 = None
        
        if not os.path.exists(DUPLICATES_CSV):
            logging.info("No duplicates CSV file found")
            return []
        
        logging.info(f"Loading duplicate groups from {DUPLICATES_CSV}")
        with open(DUPLICATES_CSV, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                sha256 = row['sha256']
                if sha256 != prev_sha256:
                    if current_group:
                        groups.append(current_group)
                        # Apply limit if specified
                        if limit is not None and len(groups) >= limit:
                            break
                        current_group = []
                current_group.append({
                    'sha256': sha256,
                    'filename': row['filename'],
                    'filepath': row['filepath'],
                    'creation_time': row['creation_time'],
                    'file_size': int(row['file_size'])
                })
                prev_sha256 = sha256
                
            # Don't forget the last group (if we haven't reached the limit)
            if current_group and (limit is None or len(groups) < limit):
                groups.append(current_group)
        
        logging.info(f"Loaded {len(groups)} duplicate groups")
        return groups