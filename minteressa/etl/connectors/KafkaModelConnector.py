from kafka import KafkaConsumer, KafkaProducer
import json


class KafkaModelConnector(object):
    """Simple wrapper class to configure a simple kafka consumer
    and producer pair, so that they can be used to perform simple
    filter() and map() operations over the received tweets"""

    def __init__(
        self,
        group_id=None,
        unlabeled_consumer_topic='consumer_limbo',
        labeled_consumer_topic='consumer_limbo',
        label_request_producer_topic='producer_limbo',
        logging_topic='minteressa_stats',
        bootstrap_servers='kafka:9092'
    ):
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.unlabeled_consumer_topic = unlabeled_consumer_topic
        self.labeled_consumer_topic = labeled_consumer_topic
        self.label_request_producer_topic = label_request_producer_topic
        self.logging_topic = logging_topic
        self.consumer = None
        self.producer = None

    def listen(self):
        for msg in self.consumer:
            print(msg)

    def connect(self):
        self.consumer = KafkaConsumer(
            (
                self.unlabeled_consumer_topic,
                self.labeled_consumer_topic
            ),
            group_id=self.group_id,
            value_deserializer=json.loads,
            bootstrap_servers=self.bootstrap_servers
        )
        # print("subscribing to %s" % self.consumer_topic)
        print("Subscribed to topic %s, %s " % (
            self.unlabeled_consumer_topic,
            self.labeled_consumer_topic
        ))

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers
        )

    def send(self, message, label_request_producer_topic=None):
        label_request_producer_topic = label_request_producer_topic \
            if label_request_producer_topic is not None \
            else self.label_request_producer_topic

        self.producer.send(label_request_producer_topic, message)

    def log(self, message, logging_topic=None):
        logging_topic = logging_topic \
            if logging_topic is not None \
            else self.logging_topic

        self.producer.send(logging_topic, message)

    def close(self):
        self.consumer.close()
        self.producer.close()
