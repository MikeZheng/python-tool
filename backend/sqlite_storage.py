import sqlite3
import os
import logging
import sys
import json
from contextlib import closing
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
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
        logging.StreamHandler(sys.stdout)
    ]
)


class SQLiteStorage(StorageInterface):
    """SQLite-based storage implementation"""

    def __init__(self):
        self.init_database()

    def init_database(self) -> None:
        """Initialize the SQLite database with required tables"""
        logging.info("Initializing SQLite database")
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # Create files table (with new columns)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    filepath TEXT UNIQUE NOT NULL,
                    creation_time TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    task_id INTEGER,
                    earliest_time TEXT,
                    time_sources TEXT,
                    file_type TEXT DEFAULT 'other',
                    is_kept BOOLEAN DEFAULT FALSE,
                    new_path TEXT
                )
            ''')

            # Create config table (singleton)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    storage_directory TEXT,
                    backup_directory TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create operation_history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    kept_file_path TEXT,
                    kept_file_new_path TEXT,
                    backup_files TEXT,
                    earliest_time TEXT,
                    file_size INTEGER,
                    space_saved INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create scan_progress table (singleton)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scan_progress (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    is_scanning BOOLEAN DEFAULT FALSE,
                    current_file TEXT,
                    processed_files INTEGER DEFAULT 0,
                    total_files INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            ''')

            # Create scan_tasks table (merged with scanned_directories)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scan_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    directory_path TEXT NOT NULL,
                    status TEXT DEFAULT 'queued',
                    scan_started_at TIMESTAMP,
                    scan_ended_at TIMESTAMP,
                    total_files INTEGER DEFAULT 0,
                    processed_files INTEGER DEFAULT 0,
                    photo_count INTEGER DEFAULT 0,
                    video_count INTEGER DEFAULT 0,
                    other_count INTEGER DEFAULT 0,
                    duplicate_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cancelled_at TIMESTAMP
                )
            ''')

            # Create scan_file_mappings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scan_file_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    file_id INTEGER NOT NULL,
                    scan_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_duplicate BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (task_id) REFERENCES scan_tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
                )
            ''')

            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_filepath ON files(filepath)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_task_id ON files(task_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_tasks_status ON scan_tasks(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_tasks_directory_path ON scan_tasks(directory_path)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_file_mappings_task_id ON scan_file_mappings(task_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_file_mappings_file_id ON scan_file_mappings(file_id)')

            # Add missing columns to existing tables (for migration)
            self._migrate_database(cursor)

            conn.commit()
        logging.info(f"Database initialized at {DB_PATH}")

    def _migrate_database(self, cursor) -> None:
        """Add missing columns for migration from older versions"""
        # Migrate files table
        cursor.execute("PRAGMA table_info(files)")
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Add new columns if they don't exist
        new_columns = [
            ('task_id', 'INTEGER'),
            ('earliest_time', 'TEXT'),
            ('time_sources', 'TEXT'),
            ('file_type', "TEXT DEFAULT 'other'"),
            ('is_kept', 'BOOLEAN DEFAULT FALSE'),
            ('new_path', 'TEXT'),
            ('mtime', 'REAL'),
        ]

        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE files ADD COLUMN {col_name} {col_type}')
                    logging.info(f"Added column {col_name} to files table")
                except sqlite3.OperationalError:
                    pass  # Column already exists

        # Migrate scan_tasks table
        cursor.execute("PRAGMA table_info(scan_tasks)")
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Add new columns if they don't exist
        new_columns = [
            ('status', "TEXT DEFAULT 'queued'"),
            ('scan_started_at', 'TIMESTAMP'),
            ('scan_ended_at', 'TIMESTAMP'),
            ('total_files', 'INTEGER DEFAULT 0'),
            ('processed_files', 'INTEGER DEFAULT 0'),
            ('photo_count', 'INTEGER DEFAULT 0'),
            ('video_count', 'INTEGER DEFAULT 0'),
            ('other_count', 'INTEGER DEFAULT 0'),
            ('duplicate_count', 'INTEGER DEFAULT 0'),
            ('error_message', 'TEXT'),
            ('cancelled_at', 'TIMESTAMP'),
            ('pause_data', 'TEXT'),
        ]

        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE scan_tasks ADD COLUMN {col_name} {col_type}')
                    logging.info(f"Added column {col_name} to scan_tasks table")
                except sqlite3.OperationalError:
                    pass  # Column already exists

        # Create indexes if they don't exist
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256)',
            'CREATE INDEX IF NOT EXISTS idx_files_filepath ON files(filepath)',
            'CREATE INDEX IF NOT EXISTS idx_files_task_id ON files(task_id)',
            'CREATE INDEX IF NOT EXISTS idx_scan_tasks_status ON scan_tasks(status)',
            'CREATE INDEX IF NOT EXISTS idx_scan_tasks_directory_path ON scan_tasks(directory_path)',
            'CREATE INDEX IF NOT EXISTS idx_scan_file_mappings_task_id ON scan_file_mappings(task_id)',
            'CREATE INDEX IF NOT EXISTS idx_scan_file_mappings_file_id ON scan_file_mappings(file_id)',
        ]

        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                logging.info(f"Created index: {index_sql.split('ON')[1].strip()}")
            except sqlite3.OperationalError:
                pass  # Index already exists

    # ==================== Existing Methods ====================

    def load_existing_file_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load existing file information from database to avoid reprocessing"""
        file_cache: Dict[str, Dict[str, Any]] = {}

        try:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.cursor()

                cursor.execute('SELECT filepath, file_size, mtime FROM files')
                rows = cursor.fetchall()

                for row in rows:
                    filepath, file_size, mtime = row
                    file_cache[filepath] = {
                        'file_size': file_size,
                        'mtime': mtime
                    }

            logging.info(f"Loaded {len(file_cache)} existing file records from database")
        except Exception as e:
            logging.warning(f"Could not load existing data from database {DB_PATH}: {e}")

        return file_cache

    def save_files(self, file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
        """Save all file information to database"""
        logging.info(f"Saving {len([f for f in file_data_list if f])} file records to database")
        with closing(sqlite3.connect(DB_PATH)) as conn:
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
        logging.info(f"Saved {inserted_count} file records to database")

    def delete_file(self, filepath: str) -> None:
        """delete one file"""
        logging.info(f"Deleting file record: {filepath}")
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM files WHERE filepath = ?', (filepath,))
            conn.commit()
        logging.info("Delete file completed")

    def get_duplicate_groups(self, limit: Optional[int] = None) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups from database for HTML viewer"""
        logging.info("Retrieving duplicate groups from database")
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT f1.sha256, f1.filename, f1.filepath, f1.creation_time, f1.file_size,
                       f1.earliest_time, f1.file_type, f1.is_kept
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
                sha256, filename, filepath, creation_time, file_size, earliest_time, file_type, is_kept = row

                row_dict = {
                    'sha256': sha256,
                    'filename': filename,
                    'filepath': filepath,
                    'creation_time': creation_time,
                    'file_size': file_size,
                    'earliest_time': earliest_time,
                    'file_type': file_type or 'other',
                    'is_kept': bool(is_kept)
                }

                if sha256 != prev_sha256:
                    if current_group:
                        groups.append(current_group)
                        current_group = []
                current_group.append(row_dict)
                prev_sha256 = sha256

            if current_group:
                groups.append(current_group)

        if limit is not None:
            groups = groups[:limit]
            logging.info(f"Retrieved {len(groups)} duplicate groups (limited to {limit})")
        else:
            logging.info(f"Retrieved {len(groups)} duplicate groups")

        return groups

    # ==================== Config Methods ====================

    def get_config(self) -> Optional[Dict[str, Any]]:
        """Get configuration from database"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT storage_directory, backup_directory, created_at, updated_at FROM config WHERE id = 1')
            row = cursor.fetchone()

        if row:
            return {
                'storage_directory': row[0],
                'backup_directory': row[1],
                'created_at': row[2],
                'updated_at': row[3]
            }
        return None

    def save_config(self, config: Dict[str, Any]) -> None:
        """Save configuration to database"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            storage_dir = config.get('storage_directory', '')
            backup_dir = config.get('backup_directory', '')

            cursor.execute('''
                INSERT OR REPLACE INTO config (id, storage_directory, backup_directory, updated_at)
                VALUES (1, ?, ?, CURRENT_TIMESTAMP)
            ''', (storage_dir, backup_dir))

            conn.commit()
        logging.info(f"Saved configuration: storage={storage_dir}, backup={backup_dir}")

    # ==================== Scanned Directories Methods ====================

    def add_scanned_directory(self, directory_path: str) -> int:
        """Add a scanned directory record, return task_id"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # Create a new scan task
            cursor.execute('''
                INSERT INTO scan_tasks (directory_path, status)
                VALUES (?, 'queued')
            ''', (directory_path,))

            task_id = cursor.lastrowid

            conn.commit()
        logging.info(f"Added scanned directory task: {directory_path} (id={task_id})")
        return task_id

    def get_scanned_directory(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific scanned directory"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, directory_path, total_files, photo_count, video_count, other_count,
                       duplicate_count, status, scan_ended_at as scanned_at
                FROM scan_tasks WHERE id = ?
            ''', (task_id,))
            row = cursor.fetchone()

        if row:
            return {
                'id': row[0],
                'directory_path': row[1],
                'total_files': row[2],
                'photo_count': row[3],
                'video_count': row[4],
                'other_count': row[5],
                'duplicate_count': row[6],
                'scan_status': row[7],
                'scanned_at': row[8]
            }
        return None

    def update_task_stats(self, task_id: int, stats: Dict[str, Any]) -> None:
        """Update task statistics after scan"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE scan_tasks
                SET total_files = ?, photo_count = ?, video_count = ?, other_count = ?,
                    duplicate_count = ?, status = 'completed', scan_ended_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                stats.get('total_files', 0),
                stats.get('photo_count', 0),
                stats.get('video_count', 0),
                stats.get('other_count', 0),
                stats.get('duplicate_count', 0),
                task_id
            ))

            conn.commit()
        logging.info(f"Updated directory stats for id={task_id}: {stats}")

    def delete_scanned_directory(self, task_id: int) -> None:
        """Delete a scanned directory and its associated files"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # Delete associated scan_file_mappings
            cursor.execute('DELETE FROM scan_file_mappings WHERE task_id = ?', (task_id,))
            # Delete associated files (if needed)
            cursor.execute('DELETE FROM files WHERE task_id = ?', (task_id,))
            # Delete task record
            cursor.execute('DELETE FROM scan_tasks WHERE id = ?', (task_id,))

            conn.commit()
        logging.info(f"Deleted scanned directory task id={task_id}")

    # ==================== Scan Progress Methods ====================

    def get_scan_progress(self) -> Optional[Dict[str, Any]]:
        """Get current scan progress"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT is_scanning, current_file, processed_files, total_files, started_at, updated_at
                FROM scan_progress WHERE id = 1
            ''')
            row = cursor.fetchone()

        if row:
            return {
                'is_scanning': bool(row[0]),
                'current_file': row[1],
                'processed_files': row[2],
                'total_files': row[3],
                'started_at': row[4],
                'updated_at': row[5]
            }
        return None

    def update_scan_progress(self, progress: Dict[str, Any]) -> None:
        """Update scan progress"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO scan_progress (id, is_scanning, current_file, processed_files,
                                                       total_files, started_at, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                progress.get('is_scanning', False),
                progress.get('current_file', ''),
                progress.get('processed_files', 0),
                progress.get('total_files', 0),
                progress.get('started_at')
            ))

            conn.commit()

    def reset_scan_progress(self) -> None:
        """Reset scan progress"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO scan_progress (id, is_scanning, current_file, processed_files, total_files)
                VALUES (1, FALSE, '', 0, 0)
            ''')

            conn.commit()

    # ==================== File Methods ====================

    def add_file(self, file_data: Dict[str, Any], task_id: Optional[int] = None) -> int:
        """Add a single file record, return file_id"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO files (filename, filepath, creation_time, file_size, sha256,
                                              task_id, earliest_time, time_sources, file_type, mtime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_data['filename'],
                file_data['filepath'],
                file_data['creation_time'],
                file_data['file_size'],
                file_data['sha256'],
                task_id,
                file_data.get('earliest_time'),
                json.dumps(file_data.get('time_sources', {})),
                file_data.get('file_type', 'other'),
                file_data.get('mtime')
            ))

            file_id = cursor.lastrowid
            conn.commit()

        return file_id

    def update_file_earliest_time(self, file_id: int, earliest_time: str, time_sources: Dict[str, Any]) -> None:
        """Update file's earliest time and time sources"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE files SET earliest_time = ?, time_sources = ? WHERE id = ?
            ''', (earliest_time, json.dumps(time_sources), file_id))

            conn.commit()

    def mark_file_kept(self, file_id: int, new_path: str) -> None:
        """Mark file as kept after deduplication"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE files SET is_kept = TRUE, new_path = ? WHERE id = ?
            ''', (new_path, file_id))

            conn.commit()

    def get_files_by_sha256(self, sha256: str) -> List[Dict[str, Any]]:
        """Get all files with given SHA256"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, filename, filepath, creation_time, file_size, sha256, earliest_time, file_type, is_kept
                FROM files WHERE sha256 = ?
            ''', (sha256,))
            rows = cursor.fetchall()

        return [{
            'id': row[0],
            'filename': row[1],
            'filepath': row[2],
            'creation_time': row[3],
            'file_size': row[4],
            'sha256': row[5],
            'earliest_time': row[6],
            'file_type': row[7] or 'other',
            'is_kept': bool(row[8])
        } for row in rows]

    def get_file_by_path(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Get file by filepath"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, filename, filepath, creation_time, file_size, sha256, earliest_time, file_type, is_kept, mtime
                FROM files WHERE filepath = ?
            ''', (filepath,))
            row = cursor.fetchone()

        if row:
            return {
                'id': row[0],
                'filename': row[1],
                'filepath': row[2],
                'creation_time': row[3],
                'file_size': row[4],
                'sha256': row[5],
                'earliest_time': row[6],
                'file_type': row[7] or 'other',
                'is_kept': bool(row[8]),
                'mtime': row[9]
            }
        return None

    def add_scan_file_mapping(self, task_id: int, file_id: int, is_duplicate: bool = False) -> None:
        """Add a scan file mapping"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO scan_file_mappings (task_id, file_id, is_duplicate)
                VALUES (?, ?, ?)
            ''', (task_id, file_id, is_duplicate))

            conn.commit()

    # ==================== Operation History Methods ====================

    def log_operation(self, operation: Dict[str, Any]) -> int:
        """Log an operation, return operation_id"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO operation_history (operation_type, sha256, kept_file_path, kept_file_new_path,
                                              backup_files, earliest_time, file_size, space_saved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                operation.get('operation_type', 'deduplicate'),
                operation.get('sha256', ''),
                operation.get('kept_file_path'),
                operation.get('kept_file_new_path'),
                json.dumps(operation.get('backup_files', [])),
                operation.get('earliest_time'),
                operation.get('file_size', 0),
                operation.get('space_saved', 0)
            ))

            operation_id = cursor.lastrowid
            conn.commit()

        logging.info(f"Logged operation: {operation.get('operation_type')} for sha256={operation.get('sha256', '')[:8]}...")
        return operation_id

    def get_operation_history(self, page: int = 1, limit: int = 20) -> List[Dict[str, Any]]:
        """Get operation history with pagination"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            offset = (page - 1) * limit

            cursor.execute('''
                SELECT id, operation_type, sha256, kept_file_path, kept_file_new_path, backup_files,
                       earliest_time, file_size, space_saved, created_at
                FROM operation_history
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            ''', (limit, offset))
            rows = cursor.fetchall()

        return [{
            'id': row[0],
            'operation_type': row[1],
            'sha256': row[2],
            'kept_file_path': row[3],
            'kept_file_new_path': row[4],
            'backup_files': json.loads(row[5]) if row[5] else [],
            'earliest_time': row[6],
            'file_size': row[7],
            'space_saved': row[8],
            'created_at': row[9]
        } for row in rows]

    def get_operation_count(self) -> int:
        """Get total operation count"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT COUNT(*) FROM operation_history')
            count = cursor.fetchone()[0]

        return count

    # ==================== Dashboard Methods ====================

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # Total files
            cursor.execute('SELECT COUNT(*) FROM files')
            total_files = cursor.fetchone()[0]

            # Photos
            cursor.execute("SELECT COUNT(*) FROM files WHERE file_type = 'photo'")
            photo_count = cursor.fetchone()[0]

            # Videos
            cursor.execute("SELECT COUNT(*) FROM files WHERE file_type = 'video'")
            video_count = cursor.fetchone()[0]

            # Duplicate groups
            cursor.execute('''
                SELECT COUNT(DISTINCT sha256) FROM files
                WHERE sha256 IN (SELECT sha256 FROM files GROUP BY sha256 HAVING COUNT(*) > 1)
            ''')
            duplicate_groups = cursor.fetchone()[0]

            # Total duplicate files
            cursor.execute('''
                SELECT COUNT(*) FROM files
                WHERE sha256 IN (SELECT sha256 FROM files GROUP BY sha256 HAVING COUNT(*) > 1)
            ''')
            duplicate_files = cursor.fetchone()[0]

            # Scanned directories
            cursor.execute('SELECT COUNT(DISTINCT directory_path) FROM scan_tasks WHERE status = "completed"')
            scanned_directories = cursor.fetchone()[0]

            # Total space saved
            cursor.execute('SELECT COALESCE(SUM(space_saved), 0) FROM operation_history')
            space_saved = cursor.fetchone()[0]

            # Total operations
            cursor.execute('SELECT COUNT(*) FROM operation_history')
            total_operations = cursor.fetchone()[0]

        return {
            'total_files': total_files,
            'photo_count': photo_count,
            'video_count': video_count,
            'other_count': total_files - photo_count - video_count,
            'duplicate_groups': duplicate_groups,
            'duplicate_files': duplicate_files,
            'scanned_directories': scanned_directories,
            'space_saved': space_saved or 0,
            'total_operations': total_operations
        }

    # ==================== Scan Tasks Methods ====================

    def add_scan_task(self, directory_path: str) -> int:
        """Add a new scan task, return task_id"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO scan_tasks (directory_path, status)
                VALUES (?, 'queued')
            ''', (directory_path,))

            task_id = cursor.lastrowid
            conn.commit()

        logging.info(f"Added scan task for directory: {directory_path} (id={task_id})")
        return task_id

    def get_scan_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific scan task"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, directory_path, status, scan_started_at, scan_ended_at,
                       total_files, processed_files, photo_count, video_count,
                       other_count, duplicate_count, error_message, created_at, pause_data
                FROM scan_tasks WHERE id = ?
            ''', (task_id,))
            row = cursor.fetchone()

        if row:
            return {
                'id': row[0],
                'directory_path': row[1],
                'status': row[2],
                'scan_started_at': row[3],
                'scan_ended_at': row[4],
                'total_files': row[5],
                'processed_files': row[6],
                'photo_count': row[7] or 0,
                'video_count': row[8] or 0,
                'other_count': row[9] or 0,
                'duplicate_count': row[10] or 0,
                'error_message': row[11],
                'created_at': row[12],
                'pause_data': row[13]
            }
        return None

    def get_scan_tasks(self) -> List[Dict[str, Any]]:
        """Get all scan tasks"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, directory_path, status, scan_started_at, scan_ended_at,
                       total_files, processed_files, photo_count, video_count,
                       other_count, duplicate_count, error_message, created_at, pause_data
                FROM scan_tasks
                ORDER BY created_at DESC
            ''')
            rows = cursor.fetchall()

        return [{
            'id': row[0],
            'directory_path': row[1],
            'status': row[2],
            'scan_started_at': row[3],
            'scan_ended_at': row[4],
            'total_files': row[5],
            'processed_files': row[6],
            'photo_count': row[7] or 0,
            'video_count': row[8] or 0,
            'other_count': row[9] or 0,
            'duplicate_count': row[10] or 0,
            'error_message': row[11],
            'created_at': row[12],
            'pause_data': row[13]
        } for row in rows]

    _ALLOWED_SCAN_TASK_COLUMNS = frozenset({
        'directory_path', 'status', 'scan_started_at', 'scan_ended_at',
        'total_files', 'processed_files', 'photo_count', 'video_count',
        'other_count', 'duplicate_count', 'error_message', 'cancelled_at',
        'pause_data'
    })

    def update_scan_task(self, task_id: int, task_data: Dict[str, Any]) -> None:
        """Update scan task information"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            set_clauses = []
            params = []

            for key, value in task_data.items():
                if key not in self._ALLOWED_SCAN_TASK_COLUMNS:
                    logging.warning(f"Rejected invalid scan_task column: {key}")
                    continue
                set_clauses.append(f"{key} = ?")
                params.append(value)

            params.append(task_id)

            if set_clauses:
                query = f"UPDATE scan_tasks SET {', '.join(set_clauses)} WHERE id = ?"
                cursor.execute(query, params)
                conn.commit()

    def delete_scan_task(self, task_id: int) -> None:
        """Delete a scan task"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('DELETE FROM scan_tasks WHERE id = ?', (task_id,))
            conn.commit()

        logging.info(f"Deleted scan task: {task_id}")

    def get_queued_tasks(self) -> List[Dict[str, Any]]:
        """Get queued scan tasks"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, directory_path, status, scan_started_at, scan_ended_at,
                       total_files, processed_files, error_message, created_at
                FROM scan_tasks
                WHERE status = 'queued'
                ORDER BY created_at ASC
            ''')
            rows = cursor.fetchall()

        return [{
            'id': row[0],
            'directory_path': row[1],
            'status': row[2],
            'scan_started_at': row[3],
            'scan_ended_at': row[4],
            'total_files': row[5],
            'processed_files': row[6],
            'error_message': row[7],
            'created_at': row[8]
        } for row in rows]

    def get_running_task(self) -> Optional[Dict[str, Any]]:
        """Get current running scan task"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, directory_path, status, scan_started_at, scan_ended_at,
                       total_files, processed_files, error_message, created_at
                FROM scan_tasks
                WHERE status = 'running'
                ORDER BY scan_started_at DESC
                LIMIT 1
            ''')
            row = cursor.fetchone()

        if row:
            return {
                'id': row[0],
                'directory_path': row[1],
                'status': row[2],
                'scan_started_at': row[3],
                'scan_ended_at': row[4],
                'total_files': row[5],
                'processed_files': row[6],
                'error_message': row[7],
                'created_at': row[8]
            }
        return None

    def cancel_task(self, task_id: int) -> None:
        """Cancel a scan task"""
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # Update task status to cancelled
            cursor.execute('''
                UPDATE scan_tasks
                SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (task_id,))

            # Delete associated scan_file_mappings
            cursor.execute('DELETE FROM scan_file_mappings WHERE task_id = ?', (task_id,))

            conn.commit()
        logging.info(f"Cancelled scan task id={task_id}")
