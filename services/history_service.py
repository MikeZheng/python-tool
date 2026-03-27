import logging
from typing import Dict, Any, List, Optional


class HistoryService:
    """Service for managing operation history"""

    def __init__(self, storage):
        """
        Initialize HistoryService

        Args:
            storage: StorageInterface implementation
        """
        self.storage = storage

    def log_operation(self, operation: Dict[str, Any]) -> int:
        """
        Log an operation

        Args:
            operation: Operation details dict

        Returns:
            Operation ID
        """
        return self.storage.log_operation(operation)

    def get_history(self, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        """
        Get operation history with pagination

        Args:
            page: Page number
            limit: Items per page

        Returns:
            Dict with history list and pagination info
        """
        operations = self.storage.get_operation_history(page, limit)
        total_count = self.storage.get_operation_count()
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        return {
            'operations': operations,
            'pagination': {
                'page': page,
                'limit': limit,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_more': page < total_pages
            }
        }

    def get_stats(self) -> Dict[str, Any]:
        """
        Get operation statistics

        Returns:
            Dict with statistics
        """
        dashboard_stats = self.storage.get_dashboard_stats()

        return {
            'total_operations': dashboard_stats.get('total_operations', 0),
            'total_space_saved': dashboard_stats.get('space_saved', 0),
            'total_files_processed': dashboard_stats.get('total_files', 0)
        }
