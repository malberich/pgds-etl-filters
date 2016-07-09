from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import sys


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
        bootstrap_servers='kafka:9092'
    ):

        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic
        self.logging_topic = logging_topic

        self.consumer = None
        self.producer = None

    def listen(self):
        while True:
            msg = self.consumer.poll()
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

    def connect(self):
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        print("subscribing to %s" % self.consumer_topic)
        self.consumer.subscribe([
            self.consumer_topic
        ])
        print("Subscribed to topic %s " % self.consumer_topic)

        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id
        })

    def send(self, message, producer_topic=None):
        producer_topic = producer_topic \
            if producer_topic is not None \
            else self.producer_topic

        self.producer.produce(
            producer_topic,
            message
        )
        # self.producer.flush()


    def log(self, message, logging_topic=None):
        logging_topic = logging_topic \
            if logging_topic is not None \
            else self.logging_topic

        self.producer.produce(logging_topic, message)
        self.producer.flush()

    def close(self):
        self.consumer.close()
        self.producer.close()
