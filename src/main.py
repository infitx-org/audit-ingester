## pip3 install confluent-kafka pyorc boto3 python-dotenv

import os
import json
import pyorc
import boto3
import io
import uuid
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
load_dotenv()  # This will load vars from .env into os.environ

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topic-event-audit")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "orc-kafka-s3-ingester")

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "audit-env1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9595")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio_secret_key")
S3_FOLDER = os.getenv("S3_FOLDER", "some-env-orc")
S3_SKIP_SSL_VERIFY = os.getenv("S3_SKIP_SSL_VERIFY", "false").lower() == "true"

# Batch Settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))


# ORC Schema (Single-Line Format)
schema = "struct<trace_id:string,span_id:string,trace_service:string,event_id:string,event_type:string,event_action:string,event_status:string,source:string,destination:string,transaction_type:string,transaction_action:string,audit_type:string,content_type:string,service_name:string,operation:string,http_method:string,http_path:string,http_query:string,http_url:string,party_id_type:string,party_identifier:string,party_sub_id_or_type:string,request_id:string,oracle_id:string,quote_id:string,transaction_id:string,conversion_request_id:string,conversion_id:string,determining_transfer_id:string,transfer_id:string,commit_request_id:string,timestamp:bigint,line:string>"

# Kafka Consumer Configuration
consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",  # Start from the beginning if no offset exists
    "enable.auto.commit": False,  # Manual commit after successful ORC write
}

# Create S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    verify=not S3_SKIP_SSL_VERIFY,
)

# Initialize Kafka Consumer
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

logging.info(f"Kafka consumer initialized. Subscribed to topic: {KAFKA_TOPIC}")

def get_s3_partitioned_key(filename, logDateTime):
    """Generate an S3 partitioned key based on timestamp."""
    s3_key = (
        f"{S3_FOLDER}/year={logDateTime.year}/month={logDateTime.month:02d}/day={logDateTime.day:02d}/hour={logDateTime.hour:02d}/"
        f"{filename}"
    )
    return s3_key

def process_batch(messages):
    """Writes a batch of Kafka messages to an ORC file"""
    if not messages:
        logging.info("No messages to process in this batch.")
        return

    batch_size = len(messages)
    logDateTime = datetime.utcnow()
    timestamp = int(logDateTime.timestamp())
    unique_id = uuid.uuid4().hex[:8]  # Short unique identifier
    filename = f"batch-{timestamp}-{unique_id}-{batch_size}.orc"
    s3_key = f"{S3_FOLDER}/{filename}"
    s3_key = get_s3_partitioned_key(filename, logDateTime)
    logging.info(f"Processing {batch_size} messages... Writing to {filename} in memory.")

    # Write ORC file to memory
    output = io.BytesIO()
    
    try:
        with pyorc.Writer(output, schema, compression=pyorc.CompressionKind.ZSTD) as writer:

            for msg in messages:
                raw_log_str = msg.value().decode("utf-8")  # Raw JSON message as string

                try:
                    rawLog = json.loads(raw_log_str)

                    # Define a mapping of target fields to their respective paths in the raw log
                    field_mappings = {
                        "trace_id": "metadata.trace.traceId",
                        "span_id": "metadata.trace.spanId",
                        "trace_service": "metadata.trace.service",
                        "event_id": "metadata.event.id",
                        "event_type": "metadata.event.type",
                        "event_action": "metadata.event.action",
                        "event_status": "metadata.event.state.status",
                        "source": "metadata.trace.tags.source",
                        "destination": "metadata.trace.tags.destination",
                        "transaction_type": "metadata.trace.tags.transactionType",
                        "transaction_action": "metadata.trace.tags.transactionAction",
                        "audit_type": "metadata.trace.tags.auditType",
                        "content_type": "metadata.trace.tags.contentType",
                        "service_name": "metadata.trace.tags.serviceName",
                        "operation": "metadata.trace.tags.operation",
                        "http_method": "metadata.trace.tags.httpMethod",
                        "http_path": "metadata.trace.tags.httpPath",
                        "http_query": "metadata.trace.tags.httpQuery",
                        "http_url": "metadata.trace.tags.httpUrl",
                        "party_id_type": "metadata.trace.tags.partyIdType",
                        "party_identifier": "metadata.trace.tags.partyIdentifier",
                        "party_sub_id_or_type": "metadata.trace.tags.partySubIdOrType",
                        "request_id": "metadata.trace.tags.requestId",
                        "oracle_id": "metadata.trace.tags.oracleId",
                        "quote_id": "metadata.trace.tags.quoteId",
                        "transaction_id": "metadata.trace.tags.transactionId",
                        "conversion_request_id": "metadata.trace.tags.conversionRequestId",
                        "conversion_id": "metadata.trace.tags.conversionId",
                        "determining_transfer_id": "metadata.trace.tags.determiningTransferId",
                        "transfer_id": "metadata.trace.tags.transferId",
                        "commit_request_id": "metadata.trace.tags.commitRequestId",
                    }

                    def get_nested_value(data, path):
                        """Helper function to safely extract a nested value from a dictionary using a dot-separated path."""
                        keys = path.split(".")
                        for key in keys:
                            if isinstance(data, dict):
                                data = data.get(key, None)
                            else:
                                return None
                        return data

                    # Update the record extraction logic
                    record = tuple(
                        get_nested_value(rawLog, path) or ""  # Use the helper function to extract each field
                        for path in field_mappings.values()
                    )

                    # Add the timestamp and raw log string to the record
                    record += (
                        int(rawLog.get("metadata", {}).get("protocol.createdAt", 0)),  # Convert timestamp to integer
                        raw_log_str,  # Full raw JSON message
                    )

                    writer.write(record)

                except json.JSONDecodeError:
                    logging.warning(f"❌ Skipping bad JSON message: {raw_log_str[:100]}...")  # Show part of bad JSON

            logging.info(f"✅ Successfully wrote {batch_size} messages to ORC file: {filename} in memory.")

        # Upload ORC file to S3
        orc_data = output.getvalue()
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=orc_data)
        logging.info(f"Uploaded ORC file to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Error writing ORC file: {e}", exc_info=True)
        raise


def consume_messages():
    """Consumes messages in batches and writes to ORC"""
    logging.info("Starting Kafka consumer loop...")
    try:
        while True:
            logging.info("Polling messages...")
            messages = consumer.consume(BATCH_SIZE, timeout=POLL_TIMEOUT)  # Fetch BATCH_SIZE messages

            if not messages:
                logging.info("No new messages received.")
                continue  # No messages received, continue polling

            try:
                process_batch(messages)
                consumer.commit(asynchronous=False)  # Commit offset after successful write
                logging.info("✅ Offsets committed.")
            except Exception as e:
                logging.error(f"❌ Skipping offset commit due to batch processing failure: {e}", exc_info=True)

    except KeyboardInterrupt:
        logging.info("🛑 Stopping Kafka consumer...")
    except Exception as e:
        logging.error(f"❌ Consumer encountered an error: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")


if __name__ == "__main__":
    consume_messages()
