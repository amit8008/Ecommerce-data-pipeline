# pip install confluent-kafka

from confluent_kafka import Producer
import socket
import time

# Setting up kafka configuration
conf = {'bootstrap.servers' :'localhost:9092',
        'client.id' :socket.gethostname()}
topic = "my-topic"

# acknowledging messages
def acked(err, msg) :
    if err is not None :
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else :
        print("Message produced: %s" % (str(msg)))


# Create kafka producer
producer = Producer(conf)

# Produce messages to kafka
for i in range(30, 40) :
    message = f"This is line number {i}"
    key = f"{i * 2}"
    producer.produce(topic, key = key, value = message, callback = acked)
    time.sleep(10)  # delay

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)
