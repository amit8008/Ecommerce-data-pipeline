# pip install confluent-kafka

from confluent_kafka import Producer
import socket
import time
import random

# Setting up kafka configuration
conf = {'bootstrap.servers' :'localhost:9092',
        'client.id' :socket.gethostname()}
topic = "my-topic"


def acked(err, msg) :
    if err is not None :
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else :
        print("Message produced: %s" % (str(msg)))


# Path to the file
file_path = "resources/booksDBsales.csv"

# Create kafka producer
producer = Producer(conf)

# Produce data line by line from file to topic
with open(file_path, mode = 'r', encoding = 'utf-8') as file :
    for line in file :
        message = line.strip()
        key = line.strip().split("|")[0]
        producer.produce(topic, key = key, value = message, callback = acked)
        # print(line.strip())  # strip() removes leading/trailing whitespace
        time.sleep(random.uniform(0.01, 0.1))

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)
