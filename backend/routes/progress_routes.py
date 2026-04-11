from flask import Blueprint, jsonify
from dependencies import get_progress_service

progress_bp = Blueprint('progress', __name__)

@progress_bp.route('/scan/progress', methods=['GET'])
def get_scan_progress():
    """Get current scan progress"""
    try:
        progress = get_progress_service().get_progress()
        return jsonify({'success': True, 'data': progress})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
