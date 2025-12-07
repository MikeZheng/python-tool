import sqlite3
import os
import logging  # Add this import
import sys       # Also needed for StreamHandler
from typing import Dict, List, Tuple, Optional, Union
from storage_base import StorageInterface


# Constants
DB_PATH: str = r"file_database.db"

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

class SQLiteStorage(StorageInterface):
    """SQLite-based storage implementation"""
    
    def __init__(self):
        self.init_database()
    
    def init_database(self) -> None:
        """Initialize the SQLite database with required tables"""
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

    
    def load_existing_file_cache(self) -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
        """Load existing file information from database to avoid reprocessing"""
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
            
            conn.close()
            logging.info(f"Loaded {len(file_cache)} existing file records from database")
        except Exception as e:
            logging.warning(f"Could not load existing data from database {DB_PATH}: {e}")
  
        
        return file_cache
    
    def save_files(self, file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
        """Save all file information to database"""
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

    
    def save_duplicates(self, duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
        """Save duplicate files information to database"""
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

    
    def refresh_duplicates(self) -> None:
        """Refresh the duplicates database by removing entries for files that no longer exist"""
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
        
        # Delete invalid entries
        deleted_count = 0
        for (sha256,) in sha256_list:
            if sha256 not in valid_sha256:
                cursor.execute('DELETE FROM duplicates WHERE sha256 = ?', (sha256,))
                deleted_count += cursor.rowcount
        
        conn.commit()
        conn.close()

    def get_duplicate_groups(self) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups from database for HTML viewer"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT sha256, filename, filepath, creation_time, file_size FROM duplicates ORDER BY sha256')
        rows = cursor.fetchall()
        
        groups = []
        current_group = []
        prev_sha256 = None
        
        for row in rows:
            sha256, filename, filepath, creation_time, file_size = row
            
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
        return groups