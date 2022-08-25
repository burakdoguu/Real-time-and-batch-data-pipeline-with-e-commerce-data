from kafka import KafkaProducer
from google.cloud import storage
import time
import json
import pandas as pd
import traceback
import logging


if __name__ =="__main__":

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('hepsi-test')
    blob = bucket.get_blob('orders.json')
    bd = blob.download_as_string()
    data = bd.decode()

    df = pd.read_json(data,lines=True)
    review_df = df[['event', 'messageid', 'userid', 'lineitems', 'orderid']]
    review_dict = review_df.to_dict(orient="records")

    try:

        for item in review_dict:
            user_id = str(item['userid'])
            line_items = item['lineitems']
            event_name = str(item['event'])
            message_id = str(item['messageid'])
            order_id = int(item['orderid'])
            timestamp = time.time()

            producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),acks='all',retries=1)
            producer.send('order-topic', {"event": event_name, "messageid": message_id, "userid": user_id, "lineitems": line_items,
                    "orderid": order_id, "timestamp": timestamp})
            producer.flush()
            time.sleep(60)
    except Exception as e:
        logging.error(traceback.format_exc())





