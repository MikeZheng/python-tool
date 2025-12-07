from flask import Flask, request, jsonify
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/delete-file', methods=['POST'])
def delete_file():
    data = request.json
    file_path = data.get('filePath')
    
    # Security check - ensure file is within allowed directories
    # You should implement proper validation here
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({'success': True, 'message': 'File deleted successfully'})
        else:
            return jsonify({'success': False, 'message': 'File not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)