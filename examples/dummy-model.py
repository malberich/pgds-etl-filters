import os
from minteressa.etl.connectors.KafkaModelConnector import KafkaModelConnector
from minteressa.model.DummyModel import DummyModel

if __name__ == '__main__':
    topic_id = os.getenv('TOPIC_MODEL_ID', None)
    if topic_id is None:
        raise ValueError("Could not get a TOPIC_MODEL_ID environment variable")

    kfk = KafkaModelConnector(
        group_id="dummy-model-%s" % topic_id,
        unlabeled_consumer_topic="unique_tweets",
        labeled_consumer_topic="labeled-%s" % topic_id,
        label_request_producer_topic="label_request-%s" % topic_id
    )

    lang_filter = DummyModel(
        connector=kfk
    )