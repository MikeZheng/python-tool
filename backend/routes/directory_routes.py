import os
import threading
import logging
from flask import Blueprint, request, jsonify
from dependencies import get_storage, get_progress_service
from tasks.scanner import scan_directory_task

directory_bp = Blueprint('directory', __name__)

@directory_bp.route('/directories', methods=['GET'])
def get_directories():
    """Get all scanned directories"""
    try:
        directories = get_storage().get_scanned_directories()
        return jsonify({'success': True, 'data': directories})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@directory_bp.route('/directories', methods=['POST'])
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


@directory_bp.route('/directories/<int:directory_id>', methods=['DELETE'])
def delete_directory(directory_id: int):
    """Delete a scanned directory"""
    try:
        get_storage().delete_scanned_directory(directory_id)
        return jsonify({'success': True, 'message': 'Directory deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@directory_bp.route('/directories/<int:directory_id>/rescan', methods=['POST'])
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
