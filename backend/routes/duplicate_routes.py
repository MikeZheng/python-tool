from flask import Blueprint, request, jsonify
from datetime import datetime
from dependencies import get_storage, get_file_ops_service

duplicate_bp = Blueprint('duplicate', __name__)

@duplicate_bp.route('/duplicates', methods=['GET'])
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


@duplicate_bp.route('/duplicates/<sha256>/deduplicate', methods=['POST'])
def deduplicate_single(sha256: str):
    """Deduplicate a single group"""
    try:
        result = get_file_ops_service().deduplicate_group(sha256)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@duplicate_bp.route('/duplicates/batch-deduplicate', methods=['POST'])
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
