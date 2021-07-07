import base64
import logging
import pandas as pd
import json
import os
from google.cloud.storage import Client

BUCKET_NAME = os.getenv('BUCKET_NAME')

class LoadToStorage:
    def __init__(self,event,context):
        self.event=event
        self.context=context
        self.bucket_name=BUCKET_NAME

    def getMsgData(self) -> str:
        logging.info("Function triggered, retrieving data")
        if "data" in self.event:
            message_chunk=base64.b64decode(self.event['Data']).decode('utf-8')
            logging.info("Datapoint validated")
            return message_chunk
        else:
            logging.error("No data found")
            return ""

    def payload_dataframe(self,message:str) -> pd.DataFrame:
        try:
            df=pd.DataFrame(json.loads(message))
            if not df.empty:
                logging.info("DataFrame created")
            else:
                logging.info("Empty DataFrame created")
        except Exception as e:
            logging.error(f"Error creating DataFrame {str(e)}")
            raise

    def uploadToBucket(self,df,filename):

        storage_client=Client()
        bucket=storage_client.bucket(self.bucket_name)
        blob=bucket.blob(f"{filename}.csv")
        blob.upload_from_string(data=df.to_csv(index=False),content_type='text/csv')
        logging.info("File uploaded to bucket")




def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    service = LoadToStorage(event,context)
    message = service.getMsgData()
    df = service.payload_dataframe(message)
    timestamp = df["price_timestamp"].unique().tolist()[0]
    service.uploadToBucket(df,"dota_pub_data_"+timestamp)