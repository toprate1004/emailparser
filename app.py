from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
from flask import make_response

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

import emailparser

app = Flask(__name__)
CORS(app, resources={r"/download_csv": {"origins": "*"}})

# Define the function to run once a day
def daily_emailparser():
    emailparser.main()
    
# Configure APScheduler in Flask
def start_scheduler():
    scheduler = BackgroundScheduler()
    # Schedule the function to run once a day at a specific time
    scheduler.add_job(daily_emailparser, 'cron', hour=0, minute=0, id="daily_emailparser_job")
    scheduler.start()

@app.route('/')
def home():
    return "Welcome to the Flask server!"

# Start the scheduler when the app starts
with app.app_context():
    start_scheduler()

@app.route('/run', methods=['POST'])
def run():
    try:
        emailparser.main()  # Call the main function in emailparser if it exists
        return "Email parser ran successfully!"
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_data', methods=['GET'])
def get_container_data():
    try:
        container_data = emailparser.get_container_data()
        return jsonify(container_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/export_csv', methods=['GET'])
def export_container_csv():
    try:
        filename = "container_list.csv"
        emailparser.export_to_csv(filename)
        
        return "CSV file was exported successfully!"
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download_csv', methods=['GET'])
def download_csv():
    # Ensure that the file exists in the specified path
    try:
        filename = "container_list.csv"
        emailparser.export_to_csv(filename)

        file_path = "./container_list.csv"
        return send_file(file_path, as_attachment=True)
    except FileNotFoundError:
        return "File not found", 404

@app.route('/redirect_to_download', methods=['GET'])
def redirect_to_download():
    return '''
    <html>
        <body>
            <script>
                window.location.href = "/download_csv";
            </script>
        </body>
    </html>
    '''

if __name__ == "__main__":
    app.run(debug=True)
