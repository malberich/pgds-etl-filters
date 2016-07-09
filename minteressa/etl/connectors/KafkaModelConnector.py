from kafka import KafkaConsumer, KafkaProducer
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import json
import sys

class KafkaModelConnector(object):
    """Simple wrapper class to configure a simple kafka consumer
    and producer pair, so that they can be used to perform simple
    filter() and map() operations over the received tweets"""

    connector_type = 'QUEUE'

    LABELED_TOPIC = 1
    UNLABELED_TOPIC = 0

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
        self.unlabeled_consumer = None
        self.labeled_consumer = None
        self.producer = None
        self.connect()

    def listen(self, topic=UNLABELED_TOPIC):
        while True:
            msg = self.listen_labeled() \
                if topic == self.LABELED_TOPIC \
                else self.listen_unlabeled()
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' % (
                            msg.topic(),
                            msg.partition(),
                            msg.offset()
                        )
                    )
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stdout.write(
                    '%s [partition-%d] at offset %d with key %s:\n' %
                    (
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        str(msg.key())
                    )
                )
                yield msg

    def listen_labeled(self, topic=UNLABELED_TOPIC):
        return self.labeled_consumer.poll()

    def listen_unlabeled(self, topic=UNLABELED_TOPIC):
        return self.unlabeled_consumer.poll()

    def connect(self):
        self.unlabeled_consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })

        self.unlabeled_consumer.subscribe([
            self.unlabeled_consumer_topic
        ])

        self.labeled_consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })

        self.labeled_consumer.subscribe([
            self.labeled_consumer_topic
        ])

        sys.stdout.write(
            "Subscribed to topic %s and %s" % (
                self.labeled_consumer_topic,
                self.unlabeled_consumer_topic
            )
        )

        self.label_request_producer = Producer(
            {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id
            }
        )
        sys.stdout.write("Producer instantiated")

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
