# Real-time-and-batch-data-pipeline-with-e-commerce-data
Google Cloud BigQuery, Storage and Dataprocs ETL Process


![image](https://user-images.githubusercontent.com/73526595/186734889-47b52581-5ae9-4518-8f92-5f3619728446.png)

# 1. Upload Files in Google Storage
All files that will be used in project have been uploaded on Google Cloud Storage.
# 2. Dataproc Cluster
Dataproc cluster was created with Google Cloud Shell;

REGION=us-central1\
CLUSTER_NAME=spark-bigquery\
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --single-node \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
    --metadata bigquery-connector-version=1.2.0 \
    --metadata spark-bigquery-connector-version=0.26.0 \
    --metadata GCS_CONNECTOR_VERSION=2.2.2

Next Step; Install kafka and should be ready to use in created VM. 
## 2.1	Kafka
•	bin/kafka-topics.sh --create --topic orders-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
•	bin/kafka-topics.sh --create --topic views-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1\
Two topics were created for products orders and views data.

Two files named as producer-order.py and producer-view.py were created on VM. These two python files are among other files in solution documents. Some modules were installed to use in producers on Cloud VM.
Messages that sent from producer-order.py file was set to every 60 seconds. On the other hand in producer-view.py, it was set to 1 second. Also some configurations of  KafkaProducer were applied such as acks = ‘all’ and retries=1 to avoid any data loss. As an example, the producer prepared for order data is shown in the following image.

# 3. Dataproc Spark

Stream and batch reading will be carried out with data received from Google Cloud Storage. Real time sent orders and views json data will be join with batch read category-map csv file. Created dataframe will be ingest into BigQuery hourly.

•Connection spark with dependencies:
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2, com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0


### Explanation Spark-Scala Scripts:

1-	import necessary modules
2-	stream reading orders-topic and views-topic messages that coming from their producers and preparation dataframes with proper schemas
3-	batch reading csv file from Storage and creating dataframe
4-	join two dataframes (like orders-category map and views-category map) 
5-	writing BigQuery hourly 









