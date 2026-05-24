import os
import re
import sys
import subprocess
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List


class TimeExtractionService:
    """Service for extracting earliest time from multiple sources"""

    # Photo extensions
    PHOTO_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.heic', '.heif', '.raw', '.cr2', '.nef'}

    # Video extensions
    VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.m4v', '.3gp'}

    # Filename time patterns
    FILENAME_PATTERNS = [
        # IMG_20231215_143000.jpg
        (r'(?:IMG|PHOTO|PIC|IMAGE)_(\d{8})_(\d{6})', '%Y%m%d%H%M%S'),
        # VID_20231215_143000.mp4
        (r'(?:VID|VIDEO|MOVIE)_(\d{8})_(\d{6})', '%Y%m%d%H%M%S'),
        # 2023-12-15 14.30.00.jpg
        (r'(\d{4})-(\d{2})-(\d{2})\s*(\d{2})\.(\d{2})\.(\d{2})', None),
        # 20231215_143000.jpg
        (r'(\d{8})_(\d{6})', '%Y%m%d%H%M%S'),
        # 20231215143000.jpg
        (r'(\d{14})', '%Y%m%d%H%M%S'),
        # photo_20231215.jpg (only date, time defaults to 00:00:00)
        (r'(?:photo|img|vid|video)_(\d{8})', '%Y%m%d'),
        # WIN_20231215_143000_Pro.jpg
        (r'WIN_(\d{8})_(\d{6})', '%Y%m%d%H%M%S'),
        # Screenshot_20231215-143000.png
        (r'Screenshot_(\d{8})-(\d{6})', '%Y%m%d%H%M%S'),
        # signal-2023-12-15-14-30-00.jpg
        (r'signal-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})', None),
    ]

    _exif_available: Optional[bool] = None
    _ffmpeg_available: Optional[bool] = None

    def __init__(self):
        """Initialize TimeExtractionService"""

    @classmethod
    def _check_exif_library(cls) -> bool:
        """Check if EXIF library is available (cached at class level)"""
        if cls._exif_available is None:
            try:
                from PIL import Image
                from PIL.ExifTags import TAGS
                cls._exif_available = True
            except ImportError:
                logging.warning("Pillow not available, EXIF extraction disabled")
                cls._exif_available = False
        return cls._exif_available

    @classmethod
    def _check_ffmpeg_library(cls) -> bool:
        """Check if ffprobe is available (cached at class level, lazy)"""
        if cls._ffmpeg_available is None:
            try:
                result = subprocess.run(
                    ['ffprobe', '-version'],
                    capture_output=True,
                    timeout=5,
                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
                )
                cls._ffmpeg_available = result.returncode == 0
                if not cls._ffmpeg_available:
                    logging.warning("ffprobe not available, video metadata extraction disabled")
            except Exception:
                logging.warning("ffprobe not available, video metadata extraction disabled")
                cls._ffmpeg_available = False
        return cls._ffmpeg_available

    def determine_file_type(self, file_path: str) -> str:
        """
        Determine file type based on extension

        Args:
            file_path: Path to file

        Returns:
            'photo', 'video', or 'other'
        """
        ext = os.path.splitext(file_path)[1].lower()

        if ext in self.PHOTO_EXTENSIONS:
            return 'photo'
        elif ext in self.VIDEO_EXTENSIONS:
            return 'video'
        else:
            return 'other'

    def extract_earliest_time(self, file_path: str) -> Tuple[Optional[datetime], Dict[str, Any]]:
        """
        Extract earliest time from multiple sources

        Args:
            file_path: Path to file

        Returns:
            Tuple of (earliest datetime, dict of time sources)
        """
        times: Dict[str, datetime] = {}
        file_type = self.determine_file_type(file_path)

        # 1. File system times
        fs_times = self.get_filesystem_times(file_path)
        times.update(fs_times)

        # 2. EXIF time (for photos)
        if file_type == 'photo':
            exif_time = self.extract_exif_datetime(file_path)
            if exif_time:
                times['exif'] = exif_time

        # 3. Video metadata (for videos)
        if file_type == 'video':
            media_time = self.extract_video_metadata_time(file_path)
            if media_time:
                times['media'] = media_time

        # 4. Filename time
        filename_time = self.parse_filename_timestamp(os.path.basename(file_path))
        if filename_time:
            times['filename'] = filename_time

        # Find earliest time
        if not times:
            return None, {'file_type': file_type}

        earliest = min(times.values())

        # Build time sources dict with ISO format strings
        time_sources = {
            'file_type': file_type,
            'earliest_source': None,
            'earliest_time': earliest.isoformat() if earliest else None
        }

        for source, dt in times.items():
            time_sources[source] = dt.isoformat() if dt else None

        # Find which source is earliest
        for source, dt in times.items():
            if dt == earliest:
                time_sources['earliest_source'] = source
                break

        return earliest, time_sources

    def get_filesystem_times(self, file_path: str) -> Dict[str, datetime]:
        """
        Get file system times

        Args:
            file_path: Path to file

        Returns:
            Dict with 'fs_created', 'fs_modified', 'fs_accessed' keys
        """
        times = {}

        try:
            stat = os.stat(file_path)

            # st_birthtime is the actual creation time where available (macOS, Python 3.12+);
            # fall back to st_ctime (correct on Windows, metadata-change time on Linux)
            try:
                birthtime = stat.st_birthtime
            except AttributeError:
                birthtime = stat.st_ctime
            times['fs_created'] = datetime.fromtimestamp(birthtime)

            # Modification time
            times['fs_modified'] = datetime.fromtimestamp(stat.st_mtime)

            # Access time
            times['fs_accessed'] = datetime.fromtimestamp(stat.st_atime)

        except Exception as e:
            logging.warning(f"Failed to get filesystem times for {file_path}: {e}")

        return times

    def extract_exif_datetime(self, file_path: str) -> Optional[datetime]:
        """
        Extract datetime from EXIF metadata

        Args:
            file_path: Path to image file

        Returns:
            datetime or None
        """
        if not self._check_exif_library():
            return None

        try:
            from PIL import Image
            from PIL.ExifTags import TAGS

            with Image.open(file_path) as img:
                exif_data = img._getexif()
                if not exif_data:
                    return None

                # Look for datetime tags
                datetime_tags = ['DateTimeOriginal', 'DateTime', 'DateTimeDigitized']

                for tag_id, value in exif_data.items():
                    tag_name = TAGS.get(tag_id, tag_id)
                    if tag_name in datetime_tags:
                        try:
                            # EXIF datetime format: "2023:12:15 14:30:00"
                            dt = datetime.strptime(value, '%Y:%m:%d %H:%M:%S')
                            return dt
                        except ValueError:
                            continue

        except Exception as e:
            logging.debug(f"Failed to extract EXIF from {file_path}: {e}")

        return None

    def extract_video_metadata_time(self, file_path: str) -> Optional[datetime]:
        """
        Extract creation time from video metadata

        Args:
            file_path: Path to video file

        Returns:
            datetime or None
        """
        if not self._check_ffmpeg_library():
            return None

        try:
            import subprocess
            import json

            # Use ffprobe to get metadata
            result = subprocess.run([
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                file_path
            ], capture_output=True, text=True, timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)

            if result.returncode != 0:
                return None

            data = json.loads(result.stdout)

            # Check format tags
            format_tags = data.get('format', {}).get('tags', {})

            # Common tag names for creation time
            time_tags = ['creation_time', 'date', 'DateTimeOriginal']

            for tag in time_tags:
                if tag in format_tags:
                    try:
                        # Try ISO format first
                        value = format_tags[tag]
                        # Handle "2023-12-15T14:30:00.000000Z" format
                        if 'T' in value:
                            value = value.replace('Z', '+00:00').split('.')[0]
                            return datetime.fromisoformat(value)
                        # Handle "2023-12-15 14:30:00" format
                        return datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
                    except (ValueError, IndexError):
                        continue

            # Check stream tags
            for stream in data.get('streams', []):
                tags = stream.get('tags', {})
                for tag in time_tags:
                    if tag in tags:
                        try:
                            value = tags[tag]
                            if 'T' in value:
                                value = value.replace('Z', '+00:00').split('.')[0]
                                return datetime.fromisoformat(value)
                            return datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
                        except (ValueError, IndexError):
                            continue

        except Exception as e:
            logging.debug(f"Failed to extract video metadata from {file_path}: {e}")

        return None

    def parse_filename_timestamp(self, filename: str) -> Optional[datetime]:
        """
        Parse timestamp from filename

        Args:
            filename: Filename to parse

        Returns:
            datetime or None
        """
        # Remove extension
        name = os.path.splitext(filename)[0]

        for pattern, date_format in self.FILENAME_PATTERNS:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                try:
                    if date_format is None:
                        # Custom extraction
                        groups = match.groups()
                        if len(groups) == 6:
                            # Year, month, day, hour, minute, second
                            dt = datetime(
                                int(groups[0]), int(groups[1]), int(groups[2]),
                                int(groups[3]), int(groups[4]), int(groups[5])
                            )
                            return dt
                    else:
                        # Concatenate matched groups and parse
                        date_str = ''.join(match.groups())
                        dt = datetime.strptime(date_str, date_format)
                        return dt
                except (ValueError, IndexError) as e:
                    logging.debug(f"Failed to parse date from filename {filename}: {e}")
                    continue

        # Try to find any 10-digit timestamp (Unix timestamp)
        timestamp_match = re.search(r'(\d{10})', name)
        if timestamp_match:
            try:
                ts = int(timestamp_match.group(1))
                # Sanity check: timestamp should be between 2000-01-01 and 2100-01-01
                if 946684800 <= ts <= 4102444800:
                    return datetime.fromtimestamp(ts)
            except (ValueError, OSError):
                pass

        return None
