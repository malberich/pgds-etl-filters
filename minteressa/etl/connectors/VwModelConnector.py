import os
import sys
import json
import collections

class VwModelConnector(object):
    """Simple wrapper class to configure a simple kafka consumer
    and producer pair, so that they can be used to perform simple
    filter() and map() operations over the received tweets"""

    connector_type = 'FILE'

    def __init__(
        self,
        group_id=None,
        unlabeled_consumer_topic='test_file.vw',
        labeled_consumer_topic='train_file.vw',
        label_request_producer_topic='test_file.vw',
        logging_topic=None,
        bootstrap_servers=None
    ):

        self.group_id = None
        self.bootstrap_servers = None

        self.unlabeled_consumer_topic = None \
            if unlabeled_consumer_topic is None \
            else unlabeled_consumer_topic

        self.labeled_consumer_topic = None \
            if labeled_consumer_topic is None \
            else labeled_consumer_topic

        self.label_request_producer_topic = None \
            if label_request_producer_topic is None \
            else label_request_producer_topic

        self.unlabeled_consumer = None
        self.labeled_consumer = None
        self.label_request_producer = None
        self.connect()

    def connect(self):
        self.unlabeled_consumer = open(self.unlabeled_consumer_topic, 'r')
        self.labeled_consumer = open(self.labeled_consumer_topic, 'r')
        if self.label_request_producer_topic is not None:
            self.label_request_producer = open(self.label_request_producer_topic, 'w+')


    def load_labeled(self):
        with self.labeled_consumer as input_data:
            for line in input_data:
                yield line

    def load_unlabeled(self):
        with self.unlabeled_consumer as input_data:
            for line in input_data:
                yield line

    def request_label(self, item, prediction = None):
        sys.stdout.write("(pred: %.3f) %s" % (prediction, item))
        sys.stdout.write("Select (1) or discard (0)? ")
        ipt = raw_input()
        return 1 if int(ipt) == 1 else -1

    def send(self, message, producer_topic=None):
        return response

    def log(self, message, logging_topic=None):
        print("%s: %s" % (logging_topic, message))

    def close(self):
        self.consumer.close()
        self.producer.close()
