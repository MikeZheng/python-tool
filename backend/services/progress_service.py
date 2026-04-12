import logging
from datetime import datetime
from typing import Dict, Any, Optional


class ProgressService:
    """Service for managing scan progress"""

    def __init__(self, storage):
        """
        Initialize ProgressService

        Args:
            storage: StorageInterface implementation
        """
        self.storage = storage

    def start_scan(self, total_files: int) -> None:
        """
        Start a new scan session

        Args:
            total_files: Total number of files to process
        """
        progress = {
            'is_scanning': True,
            'current_file': '',
            'processed_files': 0,
            'total_files': total_files,
            'started_at': datetime.now().isoformat()
        }
        self.storage.update_scan_progress(progress)
        logging.info(f"Scan started: {total_files} files to process")

    def update_progress(self, current_file: str, processed_files: int, task_id: Optional[int] = None) -> None:
        """
        Update scan progress

        Args:
            current_file: Current file being processed
            processed_files: Number of files processed so far
            task_id: Optional task ID to check status
        """
        # Check if task is cancelled
        if task_id:
            task = self.storage.get_scan_task(task_id)
            if task and task['status'] == 'cancelled':
                self.complete_scan()
                return

        existing = self.storage.get_scan_progress()
        if existing:
            progress = {
                'is_scanning': True,
                'current_file': current_file,
                'processed_files': processed_files,
                'total_files': existing.get('total_files', 0),
                'started_at': existing.get('started_at')
            }
            self.storage.update_scan_progress(progress)

    def complete_scan(self) -> None:
        """Mark scan as completed"""
        self.storage.reset_scan_progress()
        logging.info("Scan completed")

    def fail_scan(self, error: str) -> None:
        """
        Mark scan as failed

        Args:
            error: Error message
        """
        existing = self.storage.get_scan_progress()
        if existing:
            progress = {
                'is_scanning': False,
                'current_file': f"Error: {error}",
                'processed_files': existing.get('processed_files', 0),
                'total_files': existing.get('total_files', 0),
                'started_at': existing.get('started_at')
            }
            self.storage.update_scan_progress(progress)
        logging.error(f"Scan failed: {error}")

    def get_progress(self) -> Optional[Dict[str, Any]]:
        """
        Get current scan progress

        Returns:
            Progress dict or None
        """
        progress = self.storage.get_scan_progress()

        if progress:
            # Calculate percentage
            total = progress.get('total_files', 0)
            processed = progress.get('processed_files', 0)

            if total > 0:
                progress['percent'] = round((processed / total) * 100, 1)
            else:
                progress['percent'] = 0

        return progress

    def is_scanning(self) -> bool:
        """Check if a scan is in progress"""
        progress = self.storage.get_scan_progress()
        return progress.get('is_scanning', False) if progress else False

    def pause_scan(self) -> None:
        """Mark scan as paused"""
        existing = self.storage.get_scan_progress()
        if existing:
            progress = {
                'is_scanning': False,
                'current_file': 'Scan paused',
                'processed_files': existing.get('processed_files', 0),
                'total_files': existing.get('total_files', 0),
                'started_at': existing.get('started_at')
            }
            self.storage.update_scan_progress(progress)
        logging.info("Scan paused")
