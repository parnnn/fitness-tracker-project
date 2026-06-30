from datetime import datetime
import pandas as pd
import boto3
import json
import time

# Function to handle new data coming in
def handle_insert(record):
    print("Handling Insert: ", record)
    my_data = {}

    # Get the new data from DynamoDB
    new_image = record['dynamodb']['NewImage']
    
    # Loop through the data to get keys and values
    for key, value in new_image.items():
        for data_type, column_value in value.items():
            my_data[key] = column_value

    # Create a Pandas DataFrame (like an Excel table)
    dataframe = pd.DataFrame([my_data])
    dataframe['EventType'] = record['eventName']
    
    return dataframe

# Function to handle modified data
def handle_modify(record):
    print("Handling Modify: ", record)
    
    # 1. Process the new data
    new_data = {}
    for key, value in record['dynamodb']['NewImage'].items():
        for data_type, column_value in value.items():
            new_data[key] = column_value

    df_insert = pd.DataFrame([new_data])
    df_insert['EventType'] = "INSERT"

    # 2. Process the old data
    old_data = {}
    for key, value in record['dynamodb']['OldImage'].items():
        for data_type, column_value in value.items():
            old_data[key] = column_value

    df_remove = pd.DataFrame([old_data])
    df_remove['EventType'] = "REMOVE"

    # Combine both DataFrames
    return pd.concat([df_insert, df_remove], ignore_index=True)

# Function to handle deleted data
def handle_remove(record):
    print("Handle Remove: ", record)
    old_data = {}

    # Get the old data before it was deleted
    for key, value in record['dynamodb']['OldImage'].items():
        for data_type, column_value in value.items():
            old_data[key] = column_value

    dataframe = pd.DataFrame([old_data])
    dataframe['EventId'] = record['eventID']
    dataframe['EventType'] = record['eventName']
    
    return dataframe

# Setup AWS tools
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:ap-southeast-1:264384440796:FitnessTracker-Alerts'
bedrock_client = boto3.client('bedrock-runtime', region_name='ap-southeast-1')
s3_client = boto3.client('s3')

# Main function that runs when DynamoDB sends data
def lambda_handler(event, context):
    try:
        # Get the list of records from the event
        records_list = event['Records']
        
        # Loop through every record
        for record in records_list:
            
            # Check if this is a new row (INSERT)
            if record['eventName'] == 'INSERT':
                time.sleep(5)

                # 1. Get the data dictionary
                dynamodb_data = record['dynamodb']
                if 'NewImage' in dynamodb_data:
                    new_image = dynamodb_data['NewImage']
                else:
                    new_image = {}
                
                # 2. Check if heart rate exists in the data
                if 'heart_rate' in new_image:
                    heart_rate_string = new_image['heart_rate']['N']
                    heart_rate_number = int(heart_rate_string)

                    #Set a default value for ai_summary
                    new_image['ai_summary'] = {'S': 'Normal workout. AI not required.'}
                    
                    # If heart rate is 180 or higher
                    if heart_rate_number >= 180:
                        
                        # 3. Get workout details safely using if-else
                        if 'activity' in new_image:
                            activity = new_image['activity']['S']
                        else:
                            activity = 'a workout'
                            
                        if 'duration' in new_image:
                            duration = new_image['duration']['N']
                        else:
                            duration = '0'
                            
                        if 'calories' in new_image:
                            calories = new_image['calories']['N']
                        else:
                            calories = '0'

                        # --- CLAUDE 3 AI SUMMARY PART ---
                        # Create the instruction for the AI
                        prompt_text = f"Act as an elite fitness coach. Write a short, encouraging 1-sentence summary for a user who just did {activity} for {duration} minutes, burning {calories} calories with a heart rate of {heart_rate_number} BPM."
                        
                        # Setup the required format for AWS Bedrock
                        ai_request_data = {
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": 50,
                            "temperature": 0.7,
                            "messages": [
                                {
                                    "role": "user",
                                    "content": prompt_text
                                }
                            ]
                        }
                        
                        # Convert Python dictionary to JSON string
                        ai_body_string = json.dumps(ai_request_data)
                        
                        try:
                            # Ask Claude AI for the summary
                            response = bedrock_client.invoke_model(
                                body=ai_body_string,
                                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                                accept='application/json',
                                contentType='application/json'
                            )
                            
                            # Read and decode the AI's answer step-by-step
                            response_bytes = response['body'].read()
                            response_dictionary = json.loads(response_bytes)
                            
                            # Extract the actual text message from the dictionary
                            ai_message = response_dictionary['content'][0]['text']
                            ai_summary = ai_message.strip() # Remove extra spaces
                            
                        except Exception as error:
                            # If AI fails, print error and use a default message
                            print("AI Generation failed: ", error)
                            ai_summary = "Great workout! Keep it up." 
                        
                        # Add the final AI summary to our data payload
                        new_image['ai_summary'] = {'S': ai_summary}
                        # -----------------------------------

                        # --- SNS EMAIL ALERT PART ---
                        # Check who the user is
                        if 'user_id' in new_image:
                            user_name = new_image['user_id']['S']
                        else:
                            user_name = 'Unknown'
                            
                        # Create alert message
                        alert_text = f"🚨 WARNING: User {user_name} just logged a dangerously high heart rate of {heart_rate_number} BPM!"
                        
                        # Send the email alert
                        sns_client.publish(
                            TopicArn=SNS_TOPIC_ARN,
                            Message=alert_text,
                            Subject="Fitness Tracker Health Alert"
                        )
                        # -----------------------

                # 4. SAVE TO S3 PART
                # Convert our raw data to a Pandas DataFrame
                final_dataframe = handle_insert(record)
                
                # Create file name and folder path
                current_time = datetime.now()
                file_name = f"workout_log_{current_time}.csv"
                bucket_name = "fitnesstracker-staging-264384440796-ap-southeast-1-an"
                folder_path = f"staging/workout_log/{file_name}"
                
                # Convert DataFrame to CSV format
                csv_data = final_dataframe.to_csv(index=False)
                
                # Upload the CSV file to S3 Bucket
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=folder_path,
                    Body=csv_data
                )
                
        # Count how many records we finished and print it
        total_records = len(event['Records'])
        print("Successfully processed " + str(total_records) + " records.")
    
        # Return success message to AWS
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed DynamoDB records.')
        }
            
    except KeyError as error:
        # If the structure is wrong (like clicking 'Test' instead of using DynamoDB)
        print("Error: ", error)
        print("Are you using the Lambda Test button instead of DynamoDB?")
        
        return {
            'statusCode': 400,
            'body': json.dumps('Missing Records key. Trigger from DynamoDB instead.')
        }