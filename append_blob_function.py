import logging
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import azure.functions as func

def register(app):
    @app.function_name(name="append_log_to_blob")
    @app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
    def timer_trigger_append(myTimer: func.TimerRequest):
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        message = f"[{current_time}] Function ran every 1 minute.\n"
        logging.info(message)

        try:
            conn_str = os.getenv("MyBlobStorageConnection")
            container_name = "logs"
            blob_name = "log.txt"

            blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            if not blob_client.exists():
                blob_client.upload_blob(message)
            else:
                existing_data = blob_client.download_blob().readall().decode('utf-8')
                updated_data = existing_data + message
                blob_client.upload_blob(updated_data, overwrite=True)

            logging.info("Log written to blob.")
        except Exception as e:
            logging.error(f"Error writing to blob: {str(e)}")
