from minteressa.etl.connectors.KafkaConnector import KafkaModelConnector
from minteressa.model.DummyModel import DummyModel

if __name__ == '__main__':
    topic_id = "ad32e3c1"
    kfk = KafkaModelConnector(
        group_id="topic-%s" % topic_id,
        unlabeled_consumer_topic="unique_tweets",
        labeled_consumer_topic="labeled-%s" % topic_id,
        label_request_producer_topic="label_request-%s" % topic_id
    )

    lang_filter = DummyModel(
        connector=kfk
    )