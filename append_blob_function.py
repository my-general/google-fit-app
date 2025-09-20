import logging
import os
from datetime import datetime
# Import the specific clients you need
from azure.storage.blob import BlobServiceClient
import azure.functions as func

def register(app):
    # This decorator registers the function with the Function App
    @app.function_name(name="append_log_to_blob")
    # This sets the timer to run at the start of every minute
    @app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
    def timer_trigger_append(myTimer: func.TimerRequest):
        
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        message = f"[{current_time}] Function ran successfully.\n"
        
        try:
            # 1. Get the connection string from your application settings
            conn_str = os.getenv("MyBlobStorageConnection")
            
            # Fail gracefully if the setting is missing
            if not conn_str:
                logging.error("FATAL: MyBlobStorageConnection environment variable not found. Please add it in the Azure Portal.")
                return

            container_name = "logs"
            blob_name = "log.txt"

            # 2. Connect to your storage account
            blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            
            # Get a client for the specific blob
            append_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            # 3. Create the append blob if it's the very first run
            if not append_blob_client.exists():
                append_blob_client.create_append_blob()
                logging.info(f"Created new append blob: {blob_name} in container {container_name}")

            # 4. Append the new log message. This is the efficient part!
            #    No download is needed. It just adds the new data to the end.
            append_blob_client.append_block(message.encode('utf-8'))
            
            logging.info(f"Successfully appended log to {blob_name}.")

        except Exception as e:
            # Log any other errors that might occur
            logging.error(f"An unexpected error occurred: {str(e)}")
