from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Union

class StorageInterface(ABC):
    """Abstract base class for storage interfaces"""
    
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
    def refresh_duplicates(self) -> None:
        """Refresh duplicates by removing entries for non-existent files"""
        pass

    @abstractmethod
    def get_duplicate_groups(self) -> List[List[Dict[str, Union[str, int]]]]:
        """Get duplicate file groups for HTML viewer"""
        pass