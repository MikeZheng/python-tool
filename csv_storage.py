import csv
import logging
import os
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
                with open(OUTPUT_CSV, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
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
                            except (ValueError, KeyError):
                                continue
            except Exception as e:
                print(f"Warning: Could not load existing CSV file {OUTPUT_CSV}: {e}")
        
        return file_cache
    
    def save_files(self, file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
        """Write all file information to CSV"""
        headers: List[str] = ['filename', 'filepath', 'creation_time', 'file_size', 'sha256']
        
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for file_data in file_data_list:
                if file_data:
                    writer.writerow(file_data)
    
    def save_duplicates(self, duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
        """Write duplicate files information to CSV"""
        headers: List[str] = ['sha256', 'filename', 'filepath', 'creation_time', 'file_size', 'duplicate_count']
        
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
    
    def refresh_duplicates(self) -> None:
        """Refresh the duplicates CSV file by removing entries for files that no longer exist"""
        if not os.path.exists(DUPLICATES_CSV):
            return
            
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
        
        # Write back the filtered entries
        with open(DUPLICATES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(valid_entries)

    def get_duplicate_groups(self) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups from CSV for HTML viewer"""
        groups = []
        current_group = []
        prev_sha256 = None
        
        if not os.path.exists(DUPLICATES_CSV):
            return []
            
        with open(DUPLICATES_CSV, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                sha256 = row['sha256']
                if sha256 != prev_sha256:
                    if current_group:
                        groups.append(current_group)
                        current_group = []
                current_group.append({
                    'sha256': sha256,
                    'filename': row['filename'],
                    'filepath': row['filepath'],
                    'creation_time': row['creation_time'],
                    'file_size': int(row['file_size'])
                })
                prev_sha256 = sha256
                
            # Don't forget the last group
            if current_group:
                groups.append(current_group)
        
        return groups