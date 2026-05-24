import os
import threading
import logging
from datetime import datetime
from flask import Blueprint, request, jsonify
from dependencies import get_storage, get_config_service
from tasks.scanner import scan_directory_task

# Thread pool: track running tasks and max concurrency
_running_tasks: dict = {}  # task_id -> Thread
_pool_lock = threading.Lock()

task_bp = Blueprint('task', __name__)


def _get_max_concurrent() -> int:
    """Read max concurrent tasks from config (supports real-time changes)"""
    config = get_config_service().get_config()
    return max(1, int(config.get('max_concurrent_tasks', 2)))


def _claim_and_start(task_id: int, directory_path: str) -> bool:
    """Atomically claim a queued task and start it in a worker thread.

    Returns True if the task was started, False if it was already claimed
    by another scheduler or is no longer queued.
    """
    storage = get_storage()

    task = storage.get_scan_task(task_id)
    if not task or task['status'] != 'queued':
        return False

    storage.update_scan_task(task_id, {
        'status': 'running',
        'scan_started_at': datetime.now().isoformat()
    })

    with _pool_lock:
        if task_id in _running_tasks:
            return False
        thread = threading.Thread(
            target=_run_task_wrapper,
            args=(task_id, directory_path),
            daemon=True
        )
        _running_tasks[task_id] = thread

    thread.start()
    return True


def _run_task_wrapper(task_id: int, directory_path: str):
    """Wrapper that runs the scanner and handles cleanup / scheduling."""
    try:
        scan_directory_task(directory_path, task_id)
    finally:
        with _pool_lock:
            _running_tasks.pop(task_id, None)
        _schedule_queued_tasks()


def _schedule_queued_tasks():
    """Fill available pool slots with queued tasks from the database."""
    storage = get_storage()
    queued = storage.get_queued_tasks()
    if not queued:
        return

    for task in queued:
        with _pool_lock:
            available = _get_max_concurrent() - len(_running_tasks)
            if available <= 0:
                break
            if task['id'] in _running_tasks:
                continue

        _claim_and_start(task['id'], task['directory_path'])


@task_bp.route('/tasks', methods=['GET'])
def get_tasks():
    """Get all scan tasks"""
    try:
        tasks = get_storage().get_scan_tasks()
        return jsonify({'success': True, 'data': tasks})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks', methods=['POST'])
def add_task():
    """Add a new scan task"""
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'success': False, 'error': 'Invalid or missing JSON body'}), 400
        directory_path = data.get('directory')

        if not directory_path:
            return jsonify({'success': False, 'error': 'Directory path is required'}), 400

        if not os.path.exists(directory_path):
            return jsonify({'success': False, 'error': 'Directory does not exist'}), 400

        if not os.path.isdir(directory_path):
            return jsonify({'success': False, 'error': 'Path is not a directory'}), 400

        task_id = get_storage().add_scan_task(directory_path)
        _schedule_queued_tasks()

        return jsonify({
            'success': True,
            'data': {'task_id': task_id, 'message': 'Task added to queue'}
        })
    except Exception as e:
        logging.error(f"Error adding task: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id: int):
    """Get a specific scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404
        return jsonify({'success': True, 'data': task})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id: int):
    """Delete a scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404

        get_storage().cancel_task(task_id)
        get_storage().delete_scan_task(task_id)
        return jsonify({'success': True, 'message': 'Task deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>/retry', methods=['POST'])
def retry_task(task_id: int):
    """Retry a failed scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404

        if task['status'] == 'running':
            return jsonify({'success': False, 'error': 'Task is already running'}), 400

        get_storage().update_scan_task(task_id, {
            'status': 'queued',
            'scan_started_at': None,
            'scan_ended_at': None,
            'processed_files': 0,
            'photo_count': 0,
            'video_count': 0,
            'other_count': 0,
            'duplicate_count': 0,
            'error_message': None,
            'pause_data': None
        })

        _schedule_queued_tasks()
        return jsonify({'success': True, 'message': 'Task added to queue'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/queue', methods=['GET'])
def get_task_queue():
    """Get current task queue"""
    try:
        queued_tasks = get_storage().get_queued_tasks()
        return jsonify({'success': True, 'data': queued_tasks})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>/pause', methods=['POST'])
def pause_task(task_id: int):
    """Pause a running scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404

        if task['status'] != 'running':
            return jsonify({'success': False, 'error': 'Task is not running'}), 400

        get_storage().update_scan_task(task_id, {
            'status': 'paused'
        })

        return jsonify({'success': True, 'message': 'Task paused'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>/resume', methods=['POST'])
def resume_task(task_id: int):
    """Resume a paused scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404

        if task['status'] != 'paused':
            return jsonify({'success': False, 'error': 'Task is not paused'}), 400

        get_storage().update_scan_task(task_id, {
            'status': 'queued'
        })

        _schedule_queued_tasks()
        return jsonify({'success': True, 'message': 'Task resumed and added to queue'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/tasks/<int:task_id>/cancel', methods=['POST'])
def cancel_task(task_id: int):
    """Cancel a scan task"""
    try:
        task = get_storage().get_scan_task(task_id)
        if not task:
            return jsonify({'success': False, 'error': 'Task not found'}), 404

        if task['status'] in ['completed', 'failed', 'cancelled']:
            return jsonify({'success': False, 'error': 'Task cannot be cancelled'}), 400

        get_storage().cancel_task(task_id)
        return jsonify({'success': True, 'message': 'Task cancelled'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
