"""
kafka_consumer_case.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
import signal
from typing import Optional, Dict, Any


# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services
    # Note: removed missing import:
    # from utils.utils_producer import is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.sqlite_consumer_case import init_db, insert_message

#####################################
# Helpers
    #Only storing one message, getting stuck on Step 4: Process Messages
#####################################

def _topic_exists(topic: str, bootstrap: str) -> bool:
    """
    Lightweight metadata check for topic existence.
    """
    c = KafkaConsumer(
        bootstrap_servers=bootstrap,
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=3000,
        enable_auto_commit=False,
    )
    try:
        topics = c.topics()
        return topic in topics
    finally:
        try:
            c.close()
        except Exception:
            pass


def _build_consumer(topic: str, group: str, bootstrap: str) -> KafkaConsumer:
    """
    Build a consumer that BLOCKS forever (no consumer_timeout_ms).
    Prefer your project helper; fall back to direct ctor on mismatch.
    """
    # Try your helper first (signature may vary in your project)
    try:
        return create_kafka_consumer(
            topic=topic,
            group=group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except TypeError:
        # signature mismatch; fall through to direct ctor
        pass

    # Direct construction
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        # IMPORTANT: do NOT set consumer_timeout_ms here (keeps iterator infinite)
    )





#####################################
# Function to process a single message
# #####################################


def process_message(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    try:
        return  {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    # 1) Verify services (zookeeper/broker) using your project helper
    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    # 2) Ensure topic exists before building a blocking consumer
    logger.info("Step 2. Verify topic exists.")
    if not _topic_exists(topic, kafka_url):
        logger.error(f"Topic '{topic}' does not exist on {kafka_url}. Start producer or create the topic.")
        sys.exit(13)
    logger.info(f"Kafka topic '{topic}' is ready.")
    
    # 3) Build consumer (no consumer_timeout_ms) and stream forever
    logger.info("Step 3. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = _build_consumer(topic, group, kafka_url)
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 4. Process messages.(streaming). Ctrl+C to stop")

    # Graceful shutdown
    running = True
    def _stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        # The iterator blocks for new messages and does not end naturally.
        for record in consumer:
            if not running:
                break
            try:
                payload = record.value  # already deserialized dict
                processed = process_message(payload)
                if processed:
                    insert_message(processed, sql_path)
            except Exception as e:
                logger.error(f"Failed to process/insert record: {e}")
                continue
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Consumer loop ended.")



#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")
    logger.info("Using utils_config for environment variables.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
