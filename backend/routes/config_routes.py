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


@config_bp.route('/config', methods=['PUT', 'POST'])
def update_config():
    """Update configuration"""
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'success': False, 'error': 'Invalid or missing JSON body'}), 400
        storage_directory = data.get('storage_directory', '')
        backup_directory = data.get('backup_directory', '')
        max_concurrent = data.get('max_concurrent_tasks')

        config = get_config_service().save_config(
            storage_directory, backup_directory,
            max_concurrent_tasks=max_concurrent
        )

        # Notify task scheduler so new limit takes effect immediately
        from routes.task_routes import _schedule_queued_tasks
        _schedule_queued_tasks()

        return jsonify({'success': True, 'data': config})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
