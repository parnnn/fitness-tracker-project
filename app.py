from flask import Flask, render_template, request, redirect 
import datetime
import uuid

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit():
    # Check if user_id was provided, if not, generate a unique one
    user_id = request.form.get("user_id")
    if not user_id:
        user_id = f"USER-{str(uuid.uuid4())[:8]}" # Generates something like USER-7a2b9c1d
    
    # Extracting data from the form
    data = {
        "user_id": user_id,
        "activity": request.form.get("activity_type"),
        "duration": int(request.form.get("duration")),
        "calories": int(request.form.get("calories")),
        "heart_rate": int(request.form.get("heart_rate")),
        "timestamp": datetime.datetime.now().isoformat()
    }

    # LOGGING DATA (In Part 4/5, you'll replace this with DynamoDB code)
    print(f"New Fitness Log Recived: {data}")

    return "Data successfully sent to the AWS Pipline!"

app.run(debug=True)