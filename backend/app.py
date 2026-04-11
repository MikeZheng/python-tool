from flask import Flask
from flask_cors import CORS
import logging

from routes.config_routes import config_bp
from routes.directory_routes import directory_bp
from routes.duplicate_routes import duplicate_bp
from routes.history_routes import history_bp
from routes.dashboard_routes import dashboard_bp
from routes.progress_routes import progress_bp

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
app.register_blueprint(directory_bp, url_prefix='/api')
app.register_blueprint(duplicate_bp, url_prefix='/api')
app.register_blueprint(history_bp, url_prefix='/api')
app.register_blueprint(dashboard_bp, url_prefix='/api')
app.register_blueprint(progress_bp, url_prefix='/api')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
