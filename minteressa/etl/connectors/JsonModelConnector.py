import os
import sys
import json


class VwModelConnector(object):
    """Simple wrapper class to configure a simple kafka consumer
    and producer pair, so that they can be used to perform simple
    filter() and map() operations over the received tweets"""

    connector_type = 'FILE'

    def __init__(
        self,
        group_id=None,
        unlabeled_consumer_topic=None,
        labeled_consumer_topic='train_file.json',
        label_request_producer_topic='test_file.vw',
        logging_topic='minteressa_stats',
        bootstrap_servers=None
    ):

        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic
        self.logging_topic = logging_topic

        self.consumer = None
        self.producer = None

    def listen(self):
        with self.consumer as input_data:
            for line in input_data:
                yield json.loads(line)

    def connect(self):
        self.consumer = open(self.consumer_topic, 'r')
        self.producer = open(self.producer_topic, 'w+')
        print("Subscribed to topic %s " % self.consumer_topic)


    def send(self, message, producer_topic=None):
        self.producer.write(message)

    def log(self, message, logging_topic=None):
        print("%s: %s" % (logging_topic, message))

    def close(self):
        self.consumer.close()
        self.producer.close()
