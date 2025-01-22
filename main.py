import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import time

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

async def process_task(message_data):
    """
    Simulate a time-consuming task.
    """
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, _process_task, message_data)

def _process_task(message_data):
    print(f"Started processing message_data: {message_data}")
    try:
        time.sleep(5)  # Simulate a blocking task
        print(f"Message data: {message_data} processed successfully!!")
    except Exception as e:
        print(f"Error occurred while processing message_data: {message_data}. Error: {e}")

def parse_message(raw_message):
    """
    Parse the raw Kafka message into JSON format.
    """
    try:
        return json.loads(raw_message.decode("utf-8")) if raw_message else None
    except json.JSONDecodeError as error:
        print(f"Failed to decode message: {raw_message}. Error: {error}")
        return None

async def consume_from_kafka():
    """
    Connect to Kafka and consume messages asynchronously.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=parse_message
    )

    await consumer.start()
    print(f"Consuming messages from Kafka topic: {KAFKA_TOPIC_NAME}")

    try:
        async for msg in consumer:
            if msg.value:
                await process_task(msg.value)
            else:
                print(f"Received invalid message from topic {KAFKA_TOPIC_NAME}. Ignoring...")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    print("Initiating Kafka connection...")
    try:
        asyncio.run(consume_from_kafka())
    except KeyboardInterrupt:
        print("Interrupted by user. Closing connection...")
