from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Union, Any
from datetime import datetime


class StorageInterface(ABC):
    """Abstract base class for storage interfaces"""

    # ==================== Existing Methods ====================

    @abstractmethod
    def load_existing_file_cache(self) -> Dict[Tuple[str, int], Dict[str, Union[str, int]]]:
        """Load existing file information to avoid reprocessing"""
        pass

    @abstractmethod
    def save_files(self, file_data_list: List[Optional[Dict[str, Union[str, int]]]]) -> None:
        """Save all file information"""
        pass

    @abstractmethod
    def save_duplicates(self, duplicates: Dict[str, List[Dict[str, Union[str, int]]]]) -> None:
        """Save duplicate files information"""
        pass

    @abstractmethod
    def delete_file(self, filepath: str) -> None:
        """delete one file"""
        pass

    @abstractmethod
    def refresh_duplicates(self) -> None:
        """Refresh duplicates by removing entries for non-existent files"""
        pass

    @abstractmethod
    def get_duplicate_groups(self, limit: Optional[int] = None) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups for HTML viewer

        Args:
            limit (Optional[int]): Maximum number of duplicate groups to return.
                                  If None, returns all groups.
        """
        pass

    # ==================== Config Methods ====================

    @abstractmethod
    def get_config(self) -> Optional[Dict[str, Any]]:
        """Get configuration from database"""
        pass

    @abstractmethod
    def save_config(self, config: Dict[str, Any]) -> None:
        """Save configuration to database"""
        pass

    # ==================== Scanned Directories Methods ====================

    @abstractmethod
    def add_scanned_directory(self, directory_path: str) -> int:
        """Add a scanned directory record, return directory_id"""
        pass

    @abstractmethod
    def get_scanned_directories(self) -> List[Dict[str, Any]]:
        """Get all scanned directories with stats"""
        pass

    @abstractmethod
    def get_scanned_directory(self, directory_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific scanned directory"""
        pass

    @abstractmethod
    def update_directory_stats(self, directory_id: int, stats: Dict[str, Any]) -> None:
        """Update directory statistics after scan"""
        pass

    @abstractmethod
    def delete_scanned_directory(self, directory_id: int) -> None:
        """Delete a scanned directory and its associated files"""
        pass

    # ==================== Scan Progress Methods ====================

    @abstractmethod
    def get_scan_progress(self) -> Optional[Dict[str, Any]]:
        """Get current scan progress"""
        pass

    @abstractmethod
    def update_scan_progress(self, progress: Dict[str, Any]) -> None:
        """Update scan progress"""
        pass

    @abstractmethod
    def reset_scan_progress(self) -> None:
        """Reset scan progress"""
        pass

    # ==================== File Methods ====================

    @abstractmethod
    def add_file(self, file_data: Dict[str, Any], directory_id: Optional[int] = None) -> int:
        """Add a single file record, return file_id"""
        pass

    @abstractmethod
    def update_file_earliest_time(self, file_id: int, earliest_time: str, time_sources: Dict[str, Any]) -> None:
        """Update file's earliest time and time sources"""
        pass

    @abstractmethod
    def mark_file_kept(self, file_id: int, new_path: str) -> None:
        """Mark file as kept after deduplication"""
        pass

    @abstractmethod
    def get_files_by_sha256(self, sha256: str) -> List[Dict[str, Any]]:
        """Get all files with given SHA256"""
        pass

    @abstractmethod
    def get_file_by_path(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Get file by filepath"""
        pass

    # ==================== Operation History Methods ====================

    @abstractmethod
    def log_operation(self, operation: Dict[str, Any]) -> int:
        """Log an operation, return operation_id"""
        pass

    @abstractmethod
    def get_operation_history(self, page: int = 1, limit: int = 20) -> List[Dict[str, Any]]:
        """Get operation history with pagination"""
        pass

    @abstractmethod
    def get_operation_count(self) -> int:
        """Get total operation count"""
        pass

    # ==================== Dashboard Methods ====================

    @abstractmethod
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics"""
        pass

    # ==================== Scan Tasks Methods ====================

    @abstractmethod
    def add_scan_task(self, directory_path: str) -> int:
        """Add a new scan task, return task_id"""
        pass

    @abstractmethod
    def get_scan_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific scan task"""
        pass

    @abstractmethod
    def get_scan_tasks(self) -> List[Dict[str, Any]]:
        """Get all scan tasks"""
        pass

    @abstractmethod
    def update_scan_task(self, task_id: int, task_data: Dict[str, Any]) -> None:
        """Update scan task information"""
        pass

    @abstractmethod
    def delete_scan_task(self, task_id: int) -> None:
        """Delete a scan task"""
        pass

    @abstractmethod
    def get_queued_tasks(self) -> List[Dict[str, Any]]:
        """Get queued scan tasks"""
        pass

    @abstractmethod
    def get_running_task(self) -> Optional[Dict[str, Any]]:
        """Get current running scan task"""
        pass
