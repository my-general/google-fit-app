# function_app.py

import logging
import os
from datetime import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# 1. Define the FunctionApp object at the top
app = func.FunctionApp()

# 2. Use the decorator to define and register your function
@app.function_name(name="append_log_to_blob")
@app.timer_trigger(schedule="0 * * * * *", 
                   arg_name="myTimer", 
                   run_on_startup=False, 
                   use_monitor=False)
def timer_trigger_append(myTimer: func.TimerRequest):
    
    current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    message = f"[{current_time}] Function ran successfully.\n"
    
    try:
        conn_str = os.getenv("MyBlobStorageConnection")
        if not conn_str:
            logging.error("FATAL: MyBlobStorageConnection environment variable not found.")
            return

        container_name = "logs"
        blob_name = "log.txt"

        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        append_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if not append_blob_client.exists():
            append_blob_client.create_append_blob()
            logging.info(f"Created new append blob: {blob_name}")

        append_blob_client.append_block(message.encode('utf-8'))
        
        logging.info(f"Successfully appended log to {blob_name}.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
