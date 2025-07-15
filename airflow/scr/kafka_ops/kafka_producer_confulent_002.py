# pip install confluent-kafka

from confluent_kafka import Producer
import socket
import time

# Setting up kafka configuration
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}
topic = "my-topic"

# Create kafka producer
producer = Producer(conf)

# Produce messages to kafka
for i in range(20, 30):
    message = f"This is line number {i}"
    key = f"{i * 2}"
    producer.produce(topic, key=key, value=message)
    print(f"Sent: {message}")
    time.sleep(5) # delay

# close the producer
producer.flush()
