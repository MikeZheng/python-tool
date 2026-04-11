from typing import Optional
from sqlite_storage import SQLiteStorage
from storage_base import StorageInterface
from services import (
    ConfigService,
    TimeExtractionService,
    FileOperationsService,
    ProgressService,
    HistoryService
)

# Global storage instance
storage: Optional[StorageInterface] = None

# Service instances
config_service: Optional[ConfigService] = None
time_service: Optional[TimeExtractionService] = None
file_ops_service: Optional[FileOperationsService] = None
progress_service: Optional[ProgressService] = None
history_service: Optional[HistoryService] = None


def get_storage() -> StorageInterface:
    """Get storage instance (singleton)"""
    global storage
    if storage is None:
        storage = SQLiteStorage()
    return storage


def get_config_service() -> ConfigService:
    """Get ConfigService instance"""
    global config_service
    if config_service is None:
        config_service = ConfigService(get_storage())
    return config_service


def get_time_service() -> TimeExtractionService:
    """Get TimeExtractionService instance"""
    global time_service
    if time_service is None:
        time_service = TimeExtractionService()
    return time_service


def get_file_ops_service() -> FileOperationsService:
    """Get FileOperationsService instance"""
    global file_ops_service
    if file_ops_service is None:
        file_ops_service = FileOperationsService(
            get_storage(),
            get_config_service(),
            get_time_service()
        )
    return file_ops_service


def get_progress_service() -> ProgressService:
    """Get ProgressService instance"""
    global progress_service
    if progress_service is None:
        progress_service = ProgressService(get_storage())
    return progress_service


def get_history_service() -> HistoryService:
    """Get HistoryService instance"""
    global history_service
    if history_service is None:
        history_service = HistoryService(get_storage())
    return history_service
