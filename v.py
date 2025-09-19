import azure.functions as func
import logging
import os
import datetime
import json
import time

# Import Google libraries
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main(myTimer: func.TimerRequest, event: func.Out[str]) -> None:
    """
    This function runs every 15 minutes, pulls data from Google Fit,
    and sends it to an Azure Event Hub.
    """
    logging.info('Python timer trigger function executed at %s', datetime.datetime.now())

    try:
        # --- 1. AUTHENTICATE WITH GOOGLE ---
        logging.info("Attempting to authenticate with Google using stored tokens...")

        # Load secrets from the Application Settings we configured in the Azure Portal
        client_secret_info = json.loads(os.environ["GOOGLE_CLIENT_SECRET_JSON"])
        token_info = json.loads(os.environ["GOOGLE_TOKEN_JSON"])

        # The SCOPES must match the ones used to generate the token.json
        SCOPES = [
            'https://www.googleapis.com/auth/fitness.heart_rate.read',
            'https://www.googleapis.com/auth/fitness.activity.read',
            'https://www.googleapis.com/auth/fitness.nutrition.read'
        ]

        creds = Credentials.from_authorized_user_info(token_info, SCOPES)

        # Handle token refresh if necessary
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing expired Google token...")
                creds.refresh(Request())
                # In a production app, you would save the refreshed credentials.
                # For this project, the in-memory refresh is sufficient because the
                # long-lived refresh token doesn't change.
            else:
                logging.error("Could not authenticate. Token is invalid or missing refresh token.")
                return # Stop if we can't authenticate

        fitness_service = build('fitness', 'v1', credentials=creds)
        logging.info("✅ Successfully authenticated with Google Fit.")

        # --- 2. FETCH DATA FROM GOOGLE FIT ---

        # Define the time range: the last 15 minutes
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=15)
        start_time_ns = int(start_time.timestamp() * 1e9)
        end_time_ns = int(end_time.timestamp() * 1e9)
        dataset_id = f"{start_time_ns}-{end_time_ns}"

        # Define the data sources to fetch
        data_sources_to_fetch = {
            "heart_rate": "derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm",
            "steps": "derived:com.google.step_count.delta:com.google.android.gms:merge_step_deltas",
        }

        all_points = []
        for data_type, source_id in data_sources_to_fetch.items():
            try:
                logging.info(f"Fetching data for {data_type}...")
                response = fitness_service.users().dataSources().datasets().get(
                    userId='me', dataSourceId=source_id, datasetId=dataset_id).execute()

                points = response.get('point', [])
                if points:
                    logging.info(f"  - Found {len(points)} points for {data_type}.")
                    for point in points:
                        value = point.get('value', [{}])[0]
                        # Structure the data for sending
                        record = {
                            'dataType': data_type,
                            'startTime': int(point.get('startTimeNanos')),
                            'endTime': int(point.get('endTimeNanos')),
                            'value': value.get('fpVal') or value.get('intVal'),
                            'ingestionTime': datetime.datetime.utcnow().isoformat()
                        }
                        all_points.append(record)
            except HttpError as e:
                logging.warning(f"Could not fetch data for {data_type}. It might have no data. Error: {e}")

        # --- 3. SEND DATA TO EVENT HUB ---
        if all_points:
            logging.info(f"Sending {len(all_points)} total data points to the Event Hub...")
            # The output binding sends whatever is set to the 'event' parameter.
            # We must send it as a string.
            event.set(json.dumps(all_points))
            logging.info("✅ Data successfully sent to Event Hub.")
        else:
            logging.info("No new data points found in the last 15 minutes.")

    except Exception as e:
        logging.error(f"❌ A critical error occurred: {e}")
