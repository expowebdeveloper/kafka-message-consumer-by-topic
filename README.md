# Kafka Consumer

This project is a simple Kafka consumer implemented in Python using `aiokafka`. It consumes messages from a specified Kafka topic and processes them asynchronously.

## Key Features

- Connects to a Kafka cluster using environment variables.
- Consumes messages from the specified Kafka topic.
- Processes incoming messages concurrently, simulating a long-running task to demonstrate asynchronous message processing.

## Concurrency Approach

The service uses a combination of asynchronous programming and threading to handle message processing concurrently. Each message received from Kafka is processed in a separate thread using `ThreadPoolExecutor`, allowing for efficient handling of multiple messages without blocking.

## Simulated Long-Running Task

The service simulates a long-running task in the `_process_message_data` function by using `time.sleep(5)` to represent a CPU-intensive operation. This task demonstrates how the consumer can handle multiple messages concurrently without being blocked by a single long-running task.

## Python GIL Considerations

Python's Global Interpreter Lock (GIL) affects true CPU-bound tasks when using threads. In this script, the use of `ThreadPoolExecutor` allows for better performance in handling long-running tasks concurrently, compared to traditional threading in CPU-bound scenarios.

## Challenges Faced

One of the main challenges encountered during development was ensuring that the asynchronous message processing did not block the main event loop. Initially, using `time.sleep()` in the processing function caused delays in consuming new messages. To overcome this, I utilized `ThreadPoolExecutor` to run the blocking code in a separate thread, allowing the event loop to remain responsive.

Another challenge was handling message decoding errors gracefully. I implemented error handling in the `decode_kafka_message` function to ensure that any malformed messages do not crash the consumer.

## Requirements

- Python 3.7 or higher
- Kafka server
- pip for managing dependencies

## Project Structure

The project includes the following files:

```
.
├── main.py               # Main script to consume and process messages
├── .env.example          # Example environment variable file for configuration
├── requirements.txt      # List of required dependencies
└── README.md             # Project documentation
```

## Setup and Testing Instructions

### Clone the repository

Clone the repository (or create the necessary files):

```bash
git clone https://github.com/expowebdeveloper/kafka-message-consumer-by-topic.git
cd kafka-message-consumer-by-topic
```

### Install Dependencies

Ensure you have Python 3.x installed, then use pip to install the required libraries:

```bash
pip install -r requirements.txt
```

### Configure the Environment Variables

Copy the `.env.example` file to `.env` and update it with your Kafka server information:

```bash
cp .env.example .env
```

Example `.env` file:

```
KAFKA_SERVER=localhost:9092
KAFKA_CONSUMER_GROUP=my-consumer-group
KAFKA_TOPIC_NAME=your-topic-name
```

### Run the Service

Execute the `main.py` script to start consuming and processing messages:

```bash
python main.py
```

Make sure your Kafka server is running and the specified topic exists.

### Verification

To verify the output, you can check the messages in the `Scenario-Execute-Response` topic.
