from kafka import KafkaConsumer, KafkaProducer
import json


class KafkaConnector(object):
    """Simple wrapper class to configure a simple kafka consumer
    and producer pair, so that they can be used to perform simple
    filter() and map() operations over the received tweets"""

    def __init__(
        self,
        group_id=None,
        consumer_topic='consumer_limbo',
        producer_topic='consumer_limbo',
        logging_topic='minteressa_stats',
        bootstrap_servers='127.0.0.1:9092'
    ):

        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic
        self.logging_topic = logging_topic

        self.consumer = None
        self.producer = None

    def listen(self):
        for msg in self.consumer:
            print(msg)

    def connect(self):
        self.consumer = KafkaConsumer(
            self.consumer_topic,
            group_id=self.group_id,
            value_deserializer=json.loads,
            bootstrap_servers=self.bootstrap_servers
        )
        # print("subscribing to %s" % self.consumer_topic)
        print("Subscribed to topic %s " % self.consumer_topic)

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers
        )

    def send(self, message, producer_topic=None):
        producer_topic = producer_topic \
            if producer_topic is not None \
            else self.producer_topic

        self.producer.send(producer_topic, message)

    def log(self, message, logging_topic=None):
        logging_topic = logging_topic \
            if logging_topic is not None \
            else self.logging_topic

        self.producer.send(logging_topic, message)

    def close(self):
        self.consumer.close()
        self.producer.close()
