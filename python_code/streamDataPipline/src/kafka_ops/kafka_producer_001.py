#pip install kafka-python-ng

from kafka import KafkaProducer
import time

import six
import sys
if sys.version_info >= (3, 12, 3):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Define kafka broker and topic
KAFKA_BROKER = "localhost:9092"
TOPIC = "my-topic"

# Create kafka producer
producer = KafkaProducer(
    bootstrap_servers = [KAFKA_BROKER],
    value_serializer = lambda v: v.encode("utf-8"),
    key_serializer = lambda v: v.encode("utf-8")
)

# Produce messages to kafka
for i in range(10, 20):
    message = f"This is line number {i}"
    key = f"{i * 2}"
    producer.send(TOPIC,key = key, value = message)
    print(f"Sent: {message}")
    time.sleep(5) # delay

# close the producer
producer.flush()
producer.close()