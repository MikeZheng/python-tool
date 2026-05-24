import os
import logging
from typing import Dict, Any, Optional

from utils import ensure_unique_path, parse_iso_datetime


class ConfigService:
    """Service for managing application configuration"""

    def __init__(self, storage):
        """
        Initialize ConfigService

        Args:
            storage: StorageInterface implementation
        """
        self.storage = storage

    def get_config(self) -> Dict[str, Any]:
        """Get current configuration"""
        config = self.storage.get_config()
        if config is None:
            return {
                'storage_directory': '',
                'backup_directory': ''
            }
        return config

    def save_config(self, storage_directory: str, backup_directory: str) -> Dict[str, Any]:
        """
        Save configuration

        Args:
            storage_directory: Directory to store deduplicated files
            backup_directory: Directory to backup deleted files

        Returns:
            Saved configuration
        """
        # Validate directories
        if storage_directory:
            self._validate_directory(storage_directory, create=True)

        if backup_directory:
            self._validate_directory(backup_directory, create=True)

        config = {
            'storage_directory': storage_directory,
            'backup_directory': backup_directory
        }

        self.storage.save_config(config)
        logging.info(f"Configuration saved: storage={storage_directory}, backup={backup_directory}")

        return config

    def validate_storage_directory(self, path: str) -> bool:
        """
        Validate if storage directory is valid

        Args:
            path: Directory path to validate

        Returns:
            True if valid, False otherwise
        """
        return self._validate_directory(path, create=False)

    def _validate_directory(self, path: str, create: bool = False) -> bool:
        """
        Validate directory exists and is writable

        Args:
            path: Directory path
            create: If True, create directory if not exists

        Returns:
            True if valid
        """
        if not path:
            return False

        if not os.path.exists(path):
            if create:
                try:
                    os.makedirs(path, exist_ok=True)
                    logging.info(f"Created directory: {path}")
                except Exception as e:
                    raise ValueError(f"Cannot create directory {path}: {e}")
            else:
                raise ValueError(f"Directory does not exist: {path}")

        if not os.path.isdir(path):
            raise ValueError(f"Path is not a directory: {path}")

        # Check if writable
        if not os.access(path, os.W_OK):
            raise ValueError(f"Directory is not writable: {path}")

        return True

    def ensure_storage_structure(self, earliest_time_str: str) -> str:
        """
        Ensure year/month directory structure exists under storage directory

        Args:
            earliest_time_str: ISO format datetime string

        Returns:
            Path to the year/month directory
        """
        config = self.get_config()
        storage_dir = config.get('storage_directory', '')

        if not storage_dir:
            raise ValueError("Storage directory not configured")

        earliest_time = parse_iso_datetime(earliest_time_str)

        # Create year/month structure
        year = earliest_time.year
        month = f"{earliest_time.month:02d}"

        target_dir = os.path.join(storage_dir, str(year), month)
        os.makedirs(target_dir, exist_ok=True)

        logging.info(f"Ensured storage structure: {target_dir}")
        return target_dir

    def get_backup_path(self, original_path: str) -> str:
        """
        Get backup path for a file

        Args:
            original_path: Original file path

        Returns:
            Backup file path
        """
        config = self.get_config()
        backup_dir = config.get('backup_directory', '')

        if not backup_dir:
            raise ValueError("Backup directory not configured")

        # Get filename and create backup path
        filename = os.path.basename(original_path)
        backup_path = os.path.join(backup_dir, filename)

        return ensure_unique_path(backup_path)
