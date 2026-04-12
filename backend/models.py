# In a new file models.py or at the top of relevant files

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class FileInfo:
    """Represents basic file information"""
    filename: str
    filepath: str
    creation_time: str  # Consider using datetime instead
    file_size: int
    sha256: str
    
    def is_image(self) -> bool:
        """Check if file is an image based on extension"""
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff']
        return any(self.filename.lower().endswith(ext) for ext in image_extensions)

@dataclass
class DuplicateGroup:
    """Represents a group of duplicate files"""
    sha256: str
    files: List[FileInfo]
    
    @property
    def count(self) -> int:
        """Return number of duplicates in this group"""
        return len(self.files)

class PhotoGallery:
    """Manages photo collections and operations"""
    def __init__(self, files: List[FileInfo]):
        self.files = [f for f in files if f.is_image()]
    
    def sort_by_creation_time(self, descending: bool = True) -> List[FileInfo]:
        """Sort photos by creation time"""
        try:
            return sorted(
                self.files,
                key=lambda x: datetime.strptime(x.creation_time, '%Y-%m-%d %H:%M:%S'),
                reverse=descending
            )
        except ValueError:
            # Fallback if datetime parsing fails
            return sorted(self.files, key=lambda x: x.creation_time, reverse=descending)