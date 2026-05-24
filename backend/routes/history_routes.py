from flask import Blueprint, request, jsonify
from dependencies import get_history_service

history_bp = Blueprint('history', __name__)

@history_bp.route('/history', methods=['GET'])
def get_history():
    """Get operation history"""
    try:
        page = max(1, int(request.args.get('page', 1)))
        limit = max(1, min(int(request.args.get('limit', 20)), 100))

        result = get_history_service().get_history(page, limit)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
