import boto3
import json
import time
import random

import os

os.environ['AWS_ACCESS_KEY_ID'] = 'copy_key_here'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'copy_key_here'
os.environ['AWS_DEFAULT_REGION'] = 'eu-north-1'

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')

# Configuration
stream_name = "test_stream_for_purchases"

def generate_purchase_data():
    """Generate a random purchase event."""
    users = ['User1', 'User2', 'User3', 'User4', 'User5']
    return {
        "user_id": random.choice(users),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": time.time()
    }

def send_to_kinesis():
    """Send purchase data to Kinesis at random intervals."""
    while True:
        data = generate_purchase_data()
        print(f"Sending: {data}")
        response =  kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=data['user_id']
        )
        print(response)
        time.sleep(random.uniform(0.2, 1.0))  # Random interval between events

if __name__ == "__main__":
    send_to_kinesis()