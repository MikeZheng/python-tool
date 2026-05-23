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

def scan_directory_task(directory_path: str, task_id: int = None):
    """Background task to scan a directory"""
    storage = get_storage()
    time_svc = get_time_service()
    progress_svc = get_progress_service()
    
    # Update task status to running if task_id is provided
    if task_id:
        storage.update_scan_task(task_id, {
            'status': 'running',
            'scan_started_at': datetime.now().isoformat()
        })

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

    def is_file_modified(file_path: str) -> bool:
        """Check if file has been modified since last scan"""
        try:
            file_record = storage.get_file_by_path(file_path)
            if not file_record:
                return True

            stat_info = os.stat(file_path)

            if stat_info.st_size != file_record.get('file_size'):
                return True

            stored_mtime = file_record.get('mtime')
            if stored_mtime is not None and stat_info.st_mtime != stored_mtime:
                return True

            return False
        except Exception:
            return True

    try:
        logging.info(f"Starting scan for directory: {directory_path} (task_id: {task_id})")

        # Collect files
        files = collect_files(directory_path)
        total_files = len(files)

        # Update task total files if task_id is provided
        if task_id:
            storage.update_scan_task(task_id, {
                'total_files': total_files
            })

        if total_files == 0:
            storage.update_task_stats(task_id, {
                'total_files': 0,
                'photo_count': 0,
                'video_count': 0,
                'other_count': 0,
                'duplicate_count': 0
            })
            
            # Update task status if task_id is provided
            if task_id:
                storage.update_scan_task(task_id, {
                    'status': 'completed',
                    'scan_ended_at': datetime.now().isoformat(),
                    'processed_files': 0
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
        current_sha256s = set()
        current_files = []
        sha256_counts = {}

        for file_path in files:
            try:
                # Check if task is paused or cancelled
                if task_id:
                    task = storage.get_scan_task(task_id)
                    if task and task['status'] == 'paused':
                        logging.info(f"Task {task_id} has been paused")
                        progress_svc.pause_scan()
                        return
                    if task and task['status'] == 'cancelled':
                        logging.info(f"Task {task_id} has been cancelled")
                        progress_svc.complete_scan()
                        return

                # Check if file has been modified
                if not is_file_modified(file_path):
                    processed += 1
                    progress_svc.update_progress(file_path, processed, task_id)
                    continue

                # Update progress
                processed += 1
                progress_svc.update_progress(file_path, processed, task_id)

                # Update task progress if task_id is provided
                if task_id:
                    storage.update_scan_task(task_id, {
                        'processed_files': processed
                    })

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
                    'file_type': file_type,
                    'mtime': stat_info.st_mtime
                }
                file_id = storage.add_file(file_data, task_id)
                
                # Track current files for duplicate counting
                current_sha256s.add(sha256)
                current_files.append(file_data)
                
                # Check if file is duplicate
                is_duplicate = sha256 in sha256_counts
                if is_duplicate:
                    sha256_counts[sha256] += 1
                else:
                    sha256_counts[sha256] = 1
                
                # Create scan_file_mapping
                if task_id:
                    storage.add_scan_file_mapping(task_id, file_id, is_duplicate)

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")

        # Count duplicates within current scan
        duplicate_count = 0
        for count in sha256_counts.values():
            if count > 1:
                duplicate_count += count

        # Update directory stats
        stats = {
            'total_files': total_files,
            'photo_count': photo_count,
            'video_count': video_count,
            'other_count': other_count,
            'duplicate_count': duplicate_count
        }
        storage.update_task_stats(task_id, stats)

        # Update task status if task_id is provided
        if task_id:
            storage.update_scan_task(task_id, {
                'status': 'completed',
                'scan_ended_at': datetime.now().isoformat(),
                'processed_files': processed
            })

        progress_svc.complete_scan()
        logging.info(f"Completed scan for directory: {directory_path} (task_id: {task_id})")

    except Exception as e:
        error_message = str(e)
        logging.error(f"Scan failed for directory {directory_path} (task_id: {task_id}): {error_message}")
        progress_svc.fail_scan(error_message)
        
        # Update task status if task_id is provided
        if task_id:
            storage.update_scan_task(task_id, {
                'status': 'failed',
                'scan_ended_at': datetime.now().isoformat(),
                'error_message': error_message
            })
