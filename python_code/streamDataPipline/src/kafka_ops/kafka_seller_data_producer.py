# pip install confluent-kafka

from confluent_kafka import Producer
import socket
import time
import random

from src.data_simulator import seller_data_simulator
from src.utility import configuration
from src.utility.logger import logger

# Setting up kafka configuration
conf = {'bootstrap.servers' :configuration.kafka_config["bootstrap_server"],
        'client.id' :socket.gethostname()}
topic = configuration.kafka_config["seller_topic"]


def acked(err, msg) :
    if err is not None :
        logger.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else :
        logger.info("Message produced: %s" % (str(msg)))


# Path to the file ******Need to simulate data in CSV for testing***********
seller_data = configuration.data_dir + "fake_seller1.json"

# Create kafka producer
producer = Producer(conf)

# function to generate and produce the data to kafka topic
num_seller = 3
for _ in range(num_seller):
    fake_seller = seller_data_simulator.generate_fake_seller(output_type = "tuple")
    producer.produce(topic, key = fake_seller[0], value = fake_seller, callback = acked)
    time.sleep(random.uniform(0.01, 0.1))

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)
