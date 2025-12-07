import sqlite3
import os
import logging
import sys
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
        logging.info("Initializing SQLite database")
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
        logging.info(f"Saving {len([f for f in file_data_list if f])} file records to database")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute('DELETE FROM files')
        logging.debug("Cleared existing files from database")
        
        # Insert new data
        inserted_count = 0
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
                inserted_count += 1
        
        conn.commit()
        conn.close()
        logging.info(f"Saved {inserted_count} file records to database")

    
    def save_duplicates(self, duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
        """Save duplicate files information - now a no-op since we query directly"""
        logging.info("Skipping duplicate save operation - duplicates are queried directly from files table")
        pass

    
    def refresh_duplicates(self) -> None:
        """Refresh duplicates by removing entries for files that no longer exist"""
        logging.info("Refreshing files database - removing entries for non-existent files")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all files
        cursor.execute('SELECT filepath, sha256 FROM files')
        files = cursor.fetchall()
        
        # Check which files still exist
        deleted_count = 0
        for filepath, sha256 in files:
            if not os.path.exists(filepath):
                cursor.execute('DELETE FROM files WHERE filepath = ?', (filepath,))
                logging.debug(f"Removed non-existent file from database: {filepath}")
                deleted_count += 1
        
        conn.commit()
        conn.close()
        logging.info(f"Refreshed files database. Removed {deleted_count} non-existent files")

    def get_duplicate_groups(self, limit: Optional[int] = None) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups from database for HTML viewer
        
        Args:
            limit (Optional[int]): Maximum number of duplicate groups to return. 
                                If None, returns all groups.
        """
        logging.info("Retrieving duplicate groups from database")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Query files that have duplicate SHA256 hashes
        cursor.execute('''
            SELECT f1.sha256, f1.filename, f1.filepath, f1.creation_time, f1.file_size
            FROM files f1
            WHERE f1.sha256 IN (
                SELECT f2.sha256 
                FROM files f2 
                GROUP BY f2.sha256 
                HAVING COUNT(*) > 1
            )
            ORDER BY f1.sha256
        ''')
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
        
        # Apply limit if specified
        if limit is not None:
            groups = groups[:limit]
            logging.info(f"Retrieved {len(groups)} duplicate groups from database (limited to {limit})")
        else:
            logging.info(f"Retrieved {len(groups)} duplicate groups from database")
        
        return groups