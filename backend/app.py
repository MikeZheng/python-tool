from flask import Flask, send_file
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from logging.handlers import RotatingFileHandler
import os

from routes.config_routes import config_bp
from routes.duplicate_routes import duplicate_bp
from routes.history_routes import history_bp
from routes.dashboard_routes import dashboard_bp
from routes.progress_routes import progress_bp
from routes.task_routes import task_bp, _schedule_queued_tasks
from dependencies import get_storage

app = Flask(__name__)
app.config['RATELIMIT_STORAGE_URI'] = "memory://"

CORS(app)

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per minute", "10 per second"]
)

# Configure logging with rotation (max 10MB, keep 3 backups)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('file_processing.log', maxBytes=10*1024*1024, backupCount=3, encoding='utf-8'),
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
    debug = os.environ.get('FLASK_DEBUG', '0').lower() in ('1', 'true', 'yes')
    port = int(os.environ.get('FLASK_PORT', 5000))

    # Reset stuck running/paused tasks (from previous unclean shutdown) back to queued
    storage = get_storage()
    for task in storage.get_scan_tasks():
        if task['status'] in ('running', 'paused'):
            storage.update_scan_task(task['id'], {
                'status': 'queued',
                'scan_started_at': None,
                'error_message': None
            })
            logging.info(f"Reset stuck task {task['id']} ({task['directory_path']}) to queued")

    # Auto-start any queued tasks that exist on startup
    _schedule_queued_tasks()

    app.run(debug=debug, port=port)
