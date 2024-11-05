from flask import Flask, jsonify, request
import emailparser

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Flask server!"

@app.route('/run', methods=['POST'])
def run():
    try:
        print(" ========================= >>>>>>>>>>>>>>>>>>>")
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

if __name__ == "__main__":
    app.run(debug=True)
