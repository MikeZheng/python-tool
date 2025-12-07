from flask import Flask, request, jsonify
import os
from flask_cors import CORS
from csv_storage import CSVStorage
from sqlite_storage import SQLiteStorage
from pathlib import Path
import json

app = Flask(__name__)
CORS(app)

# Global storage instance - in a real app, you'd want to manage this better
storage = None


# Configuration file path
CONFIG_FILE = "config.json"

def get_storage():
    """Get appropriate storage instance based on configuration file"""
    global storage
    if storage is None:
        storage_type = "csv"  # Default storage type
        
        # Try to load storage type from configuration file
        try:
            if Path(CONFIG_FILE).exists():
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                storage_type = config.get("storage_type", "csv")
            else:
                # If no config file exists, create a default one
                config = {"storage_type": "csv"}
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(config, f, indent=2)
        except Exception as e:
            # If there's an error reading the config, use default
            print(f"Warning: Could not read config file, using default storage: {e}")
            storage_type = "csv"
        
        # Initialize appropriate storage based on config
        if storage_type == "sqlite":
            storage = SQLiteStorage()
        else:
            storage = CSVStorage()
            
    return storage

@app.route('/delete-file', methods=['POST'])
def delete_file():
    data = request.json
    file_path = data.get('filePath')
    
    # Security check - ensure file is within allowed directories
    # You should implement proper validation here
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            # Get storage instance
            storage_instance = get_storage()
            storage_instance.refresh_duplicates()
            return jsonify({'success': True, 'message': 'File deleted successfully'})
        else:
            return jsonify({'success': False, 'message': 'File not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/duplicates', methods=['GET'])
def get_duplicates():
    """
    Get duplicate file groups from storage.
    Returns 20 groups per request with pagination support.
    """
    try:
        # Get storage instance
        storage_instance = get_storage()
        
        # Get page parameter (default to 1)
        page = int(request.args.get('page', 1))
        if page < 1:
            page = 1
            
        # Get duplicate groups from storage
        all_groups = storage_instance.get_duplicate_groups()
        
        # Calculate pagination
        per_page = 20
        total_groups = len(all_groups)
        total_pages = (total_groups + per_page - 1) // per_page if total_groups > 0 else 1
        
        # Get the slice of groups for this page
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        groups = all_groups[start_index:end_index]
        
        # Prepare response
        response = {
            'success': True,
            'data': groups,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_groups': total_groups,
                'total_pages': total_pages,
                'has_more': page < total_pages
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error retrieving duplicates: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)