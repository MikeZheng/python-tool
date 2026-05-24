import os
import queue
import threading
import logging
from flask import Blueprint, request, jsonify
from dependencies import get_storage
from tasks.scanner import scan_directory_task

# Thread-safe task queue
_task_queue = queue.Queue()
_processor_started = False
_processor_lock = threading.Lock()

task_bp = Blueprint('task', __name__)


def _ensure_processor():
    """Start queue processor if not already running"""
    global _processor_started
    with _processor_lock:
        if _processor_started:
            return
        _processor_started = True
    thread = threading.Thread(target=_process_task_queue, daemon=True)
    thread.start()


def _process_task_queue():
    """Process tasks from the queue sequentially"""
    while True:
        task_id = _task_queue.get()
        try:
            task = get_storage().get_scan_task(task_id)
            if task and task['status'] == 'queued':
                directory_path = task['directory_path']
                thread = threading.Thread(
                    target=scan_directory_task,
                    args=(directory_path, task_id),
                    daemon=True
                )
                thread.start()
                thread.join()
        except Exception as e:
            logging.error(f"Error processing task {task_id}: {e}")
        finally:
            _task_queue.task_done()

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

        # Add task to database
        task_id = get_storage().add_scan_task(directory_path)

        # Add task to queue
        _task_queue.put(task_id)
        _ensure_processor()

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
        
        # Cancel in DB first; if still queued, processor will skip it
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
        
        # Reset task status
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
        
        # Add to queue
        _task_queue.put(task_id)
        _ensure_processor()

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
        
        # Update task status to paused
        get_storage().update_scan_task(task_id, {
            'status': 'paused'
        })
        
        # Scanner polls task status and will save state on next iteration

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
        
        # Update task status to queued
        get_storage().update_scan_task(task_id, {
            'status': 'queued'
        })
        
        # Add task to queue
        _task_queue.put(task_id)
        _ensure_processor()

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

        # Cancel in DB; if queued, processor will skip it on dequeue
        get_storage().cancel_task(task_id)
        
        return jsonify({'success': True, 'message': 'Task cancelled'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500