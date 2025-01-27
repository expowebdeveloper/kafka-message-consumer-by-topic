import os
import time
import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime, timedelta

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSE_TOPIC", "Scenario-Execute-Response")


async def handle_message_processing(message_data):
    """
    Simulate a time-consuming task and process the message.
    """
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        # _process_message_data is an async function, but it's run in a separate thread
        await loop.run_in_executor(
            executor, lambda: asyncio.run(_process_message_data(message_data))
        )


async def _process_message_data(message_data):
    """
    Process the message, aggregate weekly data and produce a new message to another topic.
    """
    print(f"Started processing message data: {message_data}")
    try:
        # Simulate a blocking task
        time.sleep(5)
        aggregated_data = aggregate_weekly_data(message_data["weekly"])

        await send_aggregated_data_to_kafka(
            aggregated_data, message_data["organizationId"], message_data["week_start"]
        )

        print(f"Message processed successfully!!")
    except Exception as e:
        print(
            f"Error occurred while processing message data: {message_data}. Error: {e}"
        )

def decode_kafka_message(raw_message):
    try:
        return json.loads(raw_message.decode("utf-8")) if raw_message else None
    except json.JSONDecodeError as error:
        print(f"Failed to decode message: {raw_message}. Error: {error}")
        return None


async def consume_kafka_messages():
    """
    Connect to Kafka and consume messages asynchronously.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=decode_kafka_message,
    )

    await consumer.start()
    print(f"Consuming messages from Kafka topic: {KAFKA_TOPIC_NAME}")

    try:
        async for msg in consumer:
            if msg.value:
                await handle_message_processing(msg.value)
            else:
                print(
                    f"Received invalid message from topic {KAFKA_TOPIC_NAME}. Ignoring..."
                )
    finally:
        await consumer.stop()


def aggregate_weekly_data(weekly_data):
    """
    Aggregates weekly data by Contact Type, Staff Type, and Call Center.
    """
    data_dict = defaultdict(list)

    for record in weekly_data:
        key = f"{record['Contact Type']}_{record['Staff Type']}_{record['Call Center']}"
        data_dict[key].append(record)

    result_data = {}

    for key, value in data_dict.items():
        len_of_value = len(value)
        aggregated_data = defaultdict(
            lambda: {
                "Volume": 0,
                "Abandons": 0.0,
                "Top Line Agents (FTE)": 0.0,
                "Base AHT": 0.0,
                "Handled Threshold": 0.0,
                "Service Level (X Seconds)": 0.0,
                "Acceptable Wait Time": 0.0,
                "Total Queue Time": 0.0,
                "Base AHT Sum": 0.0,
                "Service Level Sum": 0.0,
            }
        )

        for record in value:
            week_start = calculate_start_of_week(record["Week"]).date()
            aggregated_data[str(week_start)]["Volume"] += record["Volume"]
            aggregated_data[str(week_start)]["Abandons"] += record["Abandons"]
            aggregated_data[str(week_start)]["Top Line Agents (FTE)"] += record[
                "Top Line Agents (FTE)"
            ]
            aggregated_data[str(week_start)]["Base AHT Sum"] += record["Base AHT"]
            aggregated_data[str(week_start)]["Handled Threshold"] += record[
                "Handled Threshold"
            ]
            aggregated_data[str(week_start)]["Service Level Sum"] += record[
                "Service Level (X Seconds)"
            ]
            aggregated_data[str(week_start)]["Acceptable Wait Time"] += record[
                "Acceptable Wait Time"
            ]
            aggregated_data[str(week_start)]["Total Queue Time"] += record[
                "Total Queue Time"
            ]

        result_data[key] = [
            {
                "Week": str(week_start),
                "Volume": values["Volume"],
                "Abandons": values["Abandons"],
                "Top Line Agents (FTE)": values["Top Line Agents (FTE)"],
                "Base AHT": values["Base AHT Sum"] / len_of_value,
                "Handled Threshold": values["Handled Threshold"],
                "Service Level (X Seconds)": values["Service Level Sum"] / len_of_value,
                "Acceptable Wait Time": values["Acceptable Wait Time"] / len_of_value,
                "Total Queue Time": values["Total Queue Time"],
            }
            for week_start, values in aggregated_data.items()
        ]

    return result_data


def calculate_start_of_week(date_str):
    """
    Helper function to find the start of the week (Friday).
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date_obj - timedelta(days=(date_obj.weekday() + 3) % 7)
    return start_of_week


async def send_aggregated_data_to_kafka(aggregated_data, org_id, week_start_date):
    """
    Produce a message to the specified Kafka topic with the aggregated data.
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVER)

    await producer.start()
    try:
        response_payload = {
            "event": "rollup",
            "organizationId": org_id,
            "week_start": str(week_start_date),
            "weekly": aggregated_data,
        }

        await producer.send(
            KAFKA_RESPONSE_TOPIC, value=json.dumps(response_payload).encode("utf-8")
        )

        print(f"Message sent to {KAFKA_RESPONSE_TOPIC}")
    except Exception as e:
        print(f"Error sending message to {KAFKA_RESPONSE_TOPIC}: {e}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    print("Initiating Kafka connection...")
    try:
        asyncio.run(consume_kafka_messages())
    except KeyboardInterrupt:
        print("Interrupted by user. Closing connection...")
