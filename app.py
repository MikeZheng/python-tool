from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlite_storage import SQLiteStorage
from storage_base import StorageInterface
from services import (
    ConfigService,
    TimeExtractionService,
    FileOperationsService,
    ProgressService,
    HistoryService
)

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
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


# ==================== Config API ====================

@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    try:
        config = get_config_service().get_config()
        return jsonify({'success': True, 'data': config})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/config', methods=['PUT'])
def update_config():
    """Update configuration"""
    try:
        data = request.json
        storage_directory = data.get('storage_directory', '')
        backup_directory = data.get('backup_directory', '')

        config = get_config_service().save_config(storage_directory, backup_directory)
        return jsonify({'success': True, 'data': config})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Directories API ====================

@app.route('/api/directories', methods=['GET'])
def get_directories():
    """Get all scanned directories"""
    try:
        directories = get_storage().get_scanned_directories()
        return jsonify({'success': True, 'data': directories})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/directories', methods=['POST'])
def add_directory():
    """Add a new directory to scan"""
    try:
        data = request.json
        directory_path = data.get('directory')

        if not directory_path:
            return jsonify({'success': False, 'error': 'Directory path is required'}), 400

        if not os.path.exists(directory_path):
            return jsonify({'success': False, 'error': 'Directory does not exist'}), 400

        if not os.path.isdir(directory_path):
            return jsonify({'success': False, 'error': 'Path is not a directory'}), 400

        # Check if already scanning
        if get_progress_service().is_scanning():
            return jsonify({'success': False, 'error': 'Another scan is in progress'}), 400

        # Add directory record
        directory_id = get_storage().add_scanned_directory(directory_path)

        # Start scan in background
        thread = threading.Thread(target=scan_directory_task, args=(directory_id, directory_path))
        thread.daemon = True
        thread.start()

        return jsonify({
            'success': True,
            'data': {'directory_id': directory_id, 'message': 'Scan started'}
        })
    except Exception as e:
        logging.error(f"Error adding directory: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/directories/<int:directory_id>', methods=['DELETE'])
def delete_directory(directory_id: int):
    """Delete a scanned directory"""
    try:
        get_storage().delete_scanned_directory(directory_id)
        return jsonify({'success': True, 'message': 'Directory deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/directories/<int:directory_id>/rescan', methods=['POST'])
def rescan_directory(directory_id: int):
    """Rescan a specific directory"""
    try:
        directory = get_storage().get_scanned_directory(directory_id)
        if not directory:
            return jsonify({'success': False, 'error': 'Directory not found'}), 404

        if get_progress_service().is_scanning():
            return jsonify({'success': False, 'error': 'Another scan is in progress'}), 400

        # Start rescan in background
        thread = threading.Thread(
            target=scan_directory_task,
            args=(directory_id, directory['directory_path'])
        )
        thread.daemon = True
        thread.start()

        return jsonify({'success': True, 'message': 'Rescan started'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Scan Progress API ====================

@app.route('/api/scan/progress', methods=['GET'])
def get_scan_progress():
    """Get current scan progress"""
    try:
        progress = get_progress_service().get_progress()
        return jsonify({'success': True, 'data': progress})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Duplicates API ====================

@app.route('/api/duplicates', methods=['GET'])
def get_duplicates():
    """Get duplicate file groups"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))

        all_groups = get_storage().get_duplicate_groups()

        # Paginate
        total_groups = len(all_groups)
        total_pages = (total_groups + limit - 1) // limit if total_groups > 0 else 1
        start_index = (page - 1) * limit
        end_index = start_index + limit
        groups = all_groups[start_index:end_index]

        # Mark earliest file in each group
        for group in groups:
            earliest_file = None
            earliest_time = None

            for file in group:
                file_time = file.get('earliest_time') or file.get('creation_time')
                if file_time:
                    try:
                        dt = datetime.fromisoformat(file_time.replace('Z', '+00:00'))
                    except ValueError:
                        dt = datetime.strptime(file_time, '%Y-%m-%d %H:%M:%S')

                    if earliest_time is None or dt < earliest_time:
                        earliest_time = dt
                        earliest_file = file

            # Mark earliest file
            for file in group:
                file['is_earliest'] = (file == earliest_file)

        return jsonify({
            'success': True,
            'data': groups,
            'pagination': {
                'page': page,
                'limit': limit,
                'total_groups': total_groups,
                'total_pages': total_pages,
                'has_more': page < total_pages
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/<sha256>/deduplicate', methods=['POST'])
def deduplicate_single(sha256: str):
    """Deduplicate a single group"""
    try:
        result = get_file_ops_service().deduplicate_group(sha256)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/batch-deduplicate', methods=['POST'])
def batch_deduplicate():
    """Batch deduplicate multiple groups"""
    try:
        data = request.json
        sha256_list = data.get('sha256_list', [])

        if not sha256_list:
            return jsonify({'success': False, 'error': 'No SHA256 list provided'}), 400

        result = get_file_ops_service().batch_deduplicate(sha256_list)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== History API ====================

@app.route('/api/history', methods=['GET'])
def get_history():
    """Get operation history"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))

        result = get_history_service().get_history(page, limit)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Dashboard API ====================

@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        stats = get_storage().get_dashboard_stats()
        return jsonify({'success': True, 'data': stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Legacy API (for compatibility) ====================

@app.route('/delete-file', methods=['POST'])
def delete_file():
    """Delete a file (legacy endpoint)"""
    data = request.json
    file_path = data.get('filePath')

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            get_storage().delete_file(file_path)
            return jsonify({'success': True, 'message': 'File deleted successfully'})
        else:
            return jsonify({'success': False, 'message': 'File not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/duplicates', methods=['GET'])
def get_duplicates_legacy():
    """Get duplicates (legacy endpoint)"""
    try:
        page = int(request.args.get('page', 1))
        per_page = 20

        all_groups = get_storage().get_duplicate_groups()

        total_groups = len(all_groups)
        total_pages = (total_groups + per_page - 1) // per_page if total_groups > 0 else 1
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        groups = all_groups[start_index:end_index]

        return jsonify({
            'success': True,
            'data': groups,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_groups': total_groups,
                'total_pages': total_pages,
                'has_more': page < total_pages
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error retrieving duplicates: {str(e)}'
        }), 500


@app.route('/scan-directory', methods=['POST'])
def scan_directory_legacy():
    """Scan a directory (legacy endpoint)"""
    try:
        data = request.json
        directory_path = data.get('directory')

        if not directory_path:
            return jsonify({'success': False, 'message': 'No directory path provided'}), 400

        if not os.path.exists(directory_path):
            return jsonify({'success': False, 'message': 'Directory does not exist'}), 400

        # Import and call photo.py function
        from photo import scan_directories_api
        result = scan_directories_api([directory_path])

        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error scanning directory: {str(e)}'
        }), 500


# ==================== Background Tasks ====================

def scan_directory_task(directory_id: int, directory_path: str):
    """Background task to scan a directory"""
    import hashlib
    from concurrent.futures import ProcessPoolExecutor, as_completed
    import multiprocessing as mp

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


if __name__ == '__main__':
    app.run(debug=True, port=5000)
