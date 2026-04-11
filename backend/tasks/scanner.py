import os
import logging
import hashlib
from datetime import datetime
from typing import List, Optional
from dependencies import (
    get_storage,
    get_time_service,
    get_progress_service
)

def scan_directory_task(directory_id: int, directory_path: str):
    """Background task to scan a directory"""
    storage = get_storage()
    time_svc = get_time_service()
    progress_svc = get_progress_service()

    def calculate_sha256(file_path: str) -> Optional[str]:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception:
            return None

    def collect_files(dir_path: str) -> List[str]:
        """Collect all files from directory"""
        files = []
        for root, _, filenames in os.walk(dir_path):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    try:
        logging.info(f"Starting scan for directory: {directory_path}")

        # Collect files
        files = collect_files(directory_path)
        total_files = len(files)

        if total_files == 0:
            storage.update_directory_stats(directory_id, {
                'total_files': 0,
                'photo_count': 0,
                'video_count': 0,
                'other_count': 0,
                'duplicate_count': 0
            })
            progress_svc.complete_scan()
            return

        # Start progress
        progress_svc.start_scan(total_files)

        # Process files
        processed = 0
        photo_count = 0
        video_count = 0
        other_count = 0

        for file_path in files:
            try:
                # Update progress
                processed += 1
                progress_svc.update_progress(file_path, processed)

                # Get file stats
                stat_info = os.stat(file_path)
                filename = os.path.basename(file_path)
                creation_time = datetime.fromtimestamp(stat_info.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
                file_size = stat_info.st_size

                # Calculate SHA256
                sha256 = calculate_sha256(file_path)
                if not sha256:
                    continue

                # Determine file type
                file_type = time_svc.determine_file_type(file_path)
                if file_type == 'photo':
                    photo_count += 1
                elif file_type == 'video':
                    video_count += 1
                else:
                    other_count += 1

                # Extract earliest time
                earliest_dt, time_sources = time_svc.extract_earliest_time(file_path)
                earliest_time = earliest_dt.isoformat() if earliest_dt else None

                # Save to database
                file_data = {
                    'filename': filename,
                    'filepath': file_path,
                    'creation_time': creation_time,
                    'file_size': file_size,
                    'sha256': sha256,
                    'earliest_time': earliest_time,
                    'time_sources': time_sources,
                    'file_type': file_type
                }
                storage.add_file(file_data, directory_id)

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")

        # Count duplicates
        duplicate_count = 0
        groups = storage.get_duplicate_groups()
        for group in groups:
            # Check if any file in group is from this directory
            for f in group:
                file_record = storage.get_file_by_path(f['filepath'])
                if file_record:
                    duplicate_count += 1
                    break

        # Update directory stats
        storage.update_directory_stats(directory_id, {
            'total_files': total_files,
            'photo_count': photo_count,
            'video_count': video_count,
            'other_count': other_count,
            'duplicate_count': duplicate_count
        })

        progress_svc.complete_scan()
        logging.info(f"Completed scan for directory: {directory_path}")

    except Exception as e:
        logging.error(f"Scan failed for directory {directory_path}: {e}")
        progress_svc.fail_scan(str(e))
