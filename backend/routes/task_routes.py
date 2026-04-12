import os
import threading
import logging
from flask import Blueprint, request, jsonify
from dependencies import get_storage
from tasks.scanner import scan_directory_task

# Global task queue management
task_queue = []
is_task_running = False

task_bp = Blueprint('task', __name__)

def process_task_queue():
    """Process tasks in the queue"""
    global is_task_running, task_queue
    
    while task_queue:
        task_id = task_queue.pop(0)
        task = get_storage().get_scan_task(task_id)
        
        if task and task['status'] == 'queued':
            is_task_running = True
            
            # Get directory ID or create one
            directory_path = task['directory_path']
            directories = get_storage().get_scanned_directories()
            directory_id = None
            
            for dir in directories:
                if dir['directory_path'] == directory_path:
                    directory_id = dir['id']
                    break
            
            if not directory_id:
                directory_id = get_storage().add_scanned_directory(directory_path)
            
            # Start scan in background
            thread = threading.Thread(
                target=scan_directory_task,
                args=(directory_id, directory_path, task_id)
            )
            thread.daemon = True
            thread.start()
            thread.join()
            
            is_task_running = False
        
    is_task_running = False

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
        data = request.json
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
        task_queue.append(task_id)
        
        # Start processing queue if not already running
        global is_task_running
        if not is_task_running:
            thread = threading.Thread(target=process_task_queue)
            thread.daemon = True
            thread.start()

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
        
        # Remove from queue if it's not running
        global task_queue
        if task_id in task_queue:
            task_queue.remove(task_id)
        
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
            'error_message': None
        })
        
        # Add to queue
        global task_queue
        task_queue.append(task_id)
        
        # Start processing queue if not already running
        global is_task_running
        if not is_task_running:
            thread = threading.Thread(target=process_task_queue)
            thread.daemon = True
            thread.start()
        
        return jsonify({'success': True, 'message': 'Task added to queue'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/tasks/queue', methods=['GET'])
def get_task_queue():
    """Get current task queue"""
    try:
        global task_queue
        queued_tasks = []
        
        for task_id in task_queue:
            task = get_storage().get_scan_task(task_id)
            if task:
                queued_tasks.append(task)
        
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
        
        # TODO: Implement actual task pausing logic
        # This would require modifying the scanner.py to support pausing
        
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
        global task_queue
        task_queue.append(task_id)
        
        # Start processing queue if not already running
        global is_task_running
        if not is_task_running:
            thread = threading.Thread(target=process_task_queue)
            thread.daemon = True
            thread.start()
        
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
        
        # Remove from queue if it's not running
        global task_queue
        if task_id in task_queue:
            task_queue.remove(task_id)
        
        # Cancel the task
        get_storage().cancel_task(task_id)
        
        return jsonify({'success': True, 'message': 'Task cancelled'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500