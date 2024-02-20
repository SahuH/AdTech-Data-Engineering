import logging
import requests
import time

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Slack webhook URL for sending alerts
url = 'SLACK_WEBHOOK_URL'

# Function to send alert to Slack
def send_alert(message):
    payload = {'text': message}
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            logging.error(f'Failed to send Slack alert: {response.text}')
    except Exception as e:
        logging.error(f'Error sending Slack alert: {str(e)}')

# Simulate data processing with potential errors
def process_data():
    try:
        # Simulate data anomaly detection
        if time.time() % 2 == 0:
            raise ValueError('Data anomaly detected: uneven timestamp')
        
        # Simulate data processing delay
        time.sleep(2)

        # Simulate data discrepancy detection
        if time.time() % 3 == 0:
            raise ValueError('Data discrepancy detected: unexpected value')

        logging.info('Data processed successfully')
    except Exception as e:
        logging.error(f'Error processing data: {str(e)}')
        send_alert(f'Data quality issue detected: {str(e)}')

# Function for data processing loop
def main():
    while True:
        process_data()
        time.sleep(10)  # Repeat data processing every 10 seconds

if __name__ == "__main__":
    main()
