from flask import Flask, send_file
from flask_cors import CORS
import logging
import os

from routes.config_routes import config_bp
from routes.duplicate_routes import duplicate_bp
from routes.history_routes import history_bp
from routes.dashboard_routes import dashboard_bp
from routes.progress_routes import progress_bp
from routes.task_routes import task_bp

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Register blueprints
app.register_blueprint(config_bp, url_prefix='/api')
app.register_blueprint(duplicate_bp, url_prefix='/api')
app.register_blueprint(history_bp, url_prefix='/api')
app.register_blueprint(dashboard_bp, url_prefix='/api')
app.register_blueprint(progress_bp, url_prefix='/api')
app.register_blueprint(task_bp, url_prefix='/api')

@app.route('/api/files/<path:file_path>')
def serve_file(file_path):
    """Serve files for preview"""
    try:
        # 解码路径，处理URL编码的字符
        import urllib.parse
        file_path = urllib.parse.unquote(file_path)
        
        # 确保文件存在
        if not os.path.exists(file_path):
            return {'success': False, 'error': 'File not found'}, 404
        
        # 发送文件
        return send_file(file_path)
    except Exception as e:
        return {'success': False, 'error': str(e)}, 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
