from flask import Blueprint, request, jsonify
from dependencies import get_config_service

config_bp = Blueprint('config', __name__)

@config_bp.route('/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    try:
        config = get_config_service().get_config()
        return jsonify({'success': True, 'data': config})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@config_bp.route('/config', methods=['POST'])
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
