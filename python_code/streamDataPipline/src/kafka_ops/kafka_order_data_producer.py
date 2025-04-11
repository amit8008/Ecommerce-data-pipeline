# pip install confluent-kafka

from confluent_kafka import Producer
import socket
import time
import random

from src.data_simulator.order_data_simulator import generate_fake_order
from src.utility import configuration
from src.utility.logger import logger

# Setting up kafka configuration
conf = {'bootstrap.servers' :configuration.kafka_config["bootstrap_server"],
        'client.id' :socket.gethostname()}
topic = configuration.kafka_config["order_topic"]


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
num_seller = 5
for _ in range(num_seller) :
    fake_orders = generate_fake_order(configuration.data_dir + "customer_data_7.tsv",
                                      configuration.data_dir + "product_data_12.json",
                                      file_format = "raw",
                                      order_count = 3)
    for order in fake_orders:
        key = order.split("|")[0]
        print(key, type(key))
        producer.produce(topic, key = key, value = order, callback = acked)

    time.sleep(15)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)
