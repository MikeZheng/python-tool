import os
import re
import shutil
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from utils import ensure_unique_path, parse_iso_datetime


class FileOperationsService:
    """Service for file operations during deduplication"""

    def __init__(self, storage, config_service, time_extraction_service):
        """
        Initialize FileOperationsService

        Args:
            storage: StorageInterface implementation
            config_service: ConfigService instance
            time_extraction_service: TimeExtractionService instance
        """
        self.storage = storage
        self.config_service = config_service
        self.time_service = time_extraction_service

    def deduplicate_group(self, sha256: str) -> Dict[str, Any]:
        """
        Deduplicate a group of files with same SHA256

        Args:
            sha256: SHA256 hash of the file group

        Returns:
            Dict with operation result
        """
        # 1. Get all files in the group
        files = self.storage.get_files_by_sha256(sha256)

        if not files:
            return {'success': False, 'error': 'No files found for this SHA256'}

        if len(files) == 1:
            return {'success': False, 'error': 'Only one file in group, nothing to deduplicate'}

        # 2. Find earliest file (by earliest_time, then by creation_time)
        earliest_file = self._find_earliest_file(files)
        other_files = [f for f in files if f['filepath'] != earliest_file['filepath']]

        # 3. Get config
        config = self.config_service.get_config()
        storage_dir = config.get('storage_directory', '')
        backup_dir = config.get('backup_directory', '')

        if not storage_dir:
            return {'success': False, 'error': 'Storage directory not configured'}

        if not backup_dir:
            return {'success': False, 'error': 'Backup directory not configured'}

        # 4. Move other files to backup
        backup_paths = []
        for f in other_files:
            try:
                backup_path = self._backup_file(f['filepath'], backup_dir)
                backup_paths.append(backup_path)
                # Remove from database
                self.storage.delete_file(f['filepath'])
                logging.info(f"Backed up file: {f['filepath']} -> {backup_path}")
            except Exception as e:
                logging.error(f"Failed to backup file {f['filepath']}: {e}")
                return {'success': False, 'error': f'Failed to backup file: {e}'}

        # 5. Look up DB record before moving file
        file_record = self.storage.get_file_by_path(earliest_file['filepath'])

        # 6. Move earliest file to storage
        earliest_time = earliest_file.get('earliest_time')
        if not earliest_time:
            dt, time_sources = self.time_service.extract_earliest_time(earliest_file['filepath'])
            if dt:
                earliest_time = dt.isoformat()
            else:
                earliest_time = earliest_file['creation_time']

        try:
            new_path = self._move_to_storage(
                earliest_file['filepath'],
                earliest_time,
                storage_dir
            )
        except Exception as e:
            logging.error(f"Failed to move file to storage: {e}")
            return {'success': False, 'error': f'Failed to move file to storage: {e}'}

        # 7. Update database — mark kept with new path instead of deleting
        if file_record:
            self.storage.mark_file_kept(file_record['id'], new_path)
        else:
            self.storage.delete_file(earliest_file['filepath'])

        # 7. Log operation
        file_size = earliest_file['file_size']
        space_saved = file_size * len(other_files)  # Space saved by deduplication

        operation = {
            'operation_type': 'deduplicate',
            'sha256': sha256,
            'kept_file_path': earliest_file['filepath'],
            'kept_file_new_path': new_path,
            'backup_files': backup_paths,
            'earliest_time': earliest_time,
            'file_size': file_size,
            'space_saved': space_saved
        }
        self.storage.log_operation(operation)

        return {
            'success': True,
            'kept_file': earliest_file['filepath'],
            'new_path': new_path,
            'backup_files': backup_paths,
            'space_saved': space_saved
        }

    def _find_earliest_file(self, files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Find the file with earliest time"""
        def get_sort_key(f):
            earliest = f.get('earliest_time')
            if earliest:
                try:
                    return datetime.fromisoformat(earliest.replace('Z', '+00:00'))
                except ValueError:
                    pass
            # Fallback to creation_time
            try:
                return datetime.strptime(f['creation_time'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return datetime.max

        return min(files, key=get_sort_key)

    def _backup_file(self, file_path: str, backup_dir: str) -> str:
        """
        Move file to backup directory

        Args:
            file_path: Original file path
            backup_dir: Backup directory

        Returns:
            New file path in backup directory
        """
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir, exist_ok=True)

        filename = os.path.basename(file_path)
        backup_path = os.path.join(backup_dir, filename)

        backup_path = ensure_unique_path(backup_path)
        shutil.move(file_path, backup_path)
        return backup_path

    def _move_to_storage(self, file_path: str, earliest_time_str: str, storage_dir: str) -> str:
        """
        Move file to storage directory with year/month structure

        Args:
            file_path: Original file path
            earliest_time_str: ISO format datetime string
            storage_dir: Storage directory

        Returns:
            New file path
        """
        earliest_time = parse_iso_datetime(earliest_time_str)

        # Create year/month structure
        year = str(earliest_time.year)
        month = f"{earliest_time.month:02d}"

        target_dir = os.path.join(storage_dir, year, month)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        # Generate new filename
        original_name = os.path.basename(file_path)
        new_name = self._generate_new_filename(original_name, earliest_time)
        new_path = os.path.join(target_dir, new_name)

        new_path = ensure_unique_path(new_path)
        shutil.move(file_path, new_path)
        logging.info(f"Moved file: {file_path} -> {new_path}")
        return new_path

    def _generate_new_filename(self, original_name: str, earliest_time: datetime) -> str:
        """
        Generate new filename with timestamp

        Args:
            original_name: Original filename
            earliest_time: Earliest datetime

        Returns:
            New filename
        """
        base, ext = os.path.splitext(original_name)

        # Remove existing timestamp patterns from filename
        # Patterns to remove: _20231215_143000, 20231215143000, _20231215, etc.
        patterns_to_remove = [
            r'_?\d{14}_?',      # _20231215143000_
            r'_?\d{8}_\d{6}',   # _20231215_143000
            r'_?\d{8}',         # _20231215
            r'_\d{10}',         # Unix timestamp
        ]

        clean_base = base
        for pattern in patterns_to_remove:
            clean_base = re.sub(pattern, '', clean_base, flags=re.IGNORECASE)

        # Remove trailing underscores or hyphens
        clean_base = clean_base.rstrip('_-')

        # If clean_base is empty, use original base
        if not clean_base:
            clean_base = base

        # Generate timestamp suffix
        timestamp_suffix = earliest_time.strftime('%Y%m%d%H%M%S')

        # New filename: clean_base_timestamp.ext
        new_name = f"{clean_base}_{timestamp_suffix}{ext}"

        return new_name

    def batch_deduplicate(self, sha256_list: List[str]) -> Dict[str, Any]:
        """
        Deduplicate multiple groups

        Args:
            sha256_list: List of SHA256 hashes

        Returns:
            Dict with batch operation results
        """
        results = []
        success_count = 0
        error_count = 0
        total_space_saved = 0

        for sha256 in sha256_list:
            result = self.deduplicate_group(sha256)
            results.append({
                'sha256': sha256,
                'success': result.get('success', False),
                'error': result.get('error')
            })

            if result.get('success'):
                success_count += 1
                total_space_saved += result.get('space_saved', 0)
            else:
                error_count += 1

        return {
            'success': True,
            'total_groups': len(sha256_list),
            'success_count': success_count,
            'error_count': error_count,
            'total_space_saved': total_space_saved,
            'results': results
        }
