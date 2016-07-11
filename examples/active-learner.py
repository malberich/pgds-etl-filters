import os

from minteressa.etl.connectors.KafkaModelConnector import KafkaModelConnector
from minteressa.etl.mappers.VowpalFormatter import VowpalFormatter
from minteressa.model.ActiveLearnerModel import ActiveLearnerModel

if __name__ == '__main__':
    topic_id = os.getenv('TOPIC_MODEL_ID', 'topic1234')
    if topic_id is None:
        raise ValueError("Could not get a TOPIC_MODEL_ID environment variable")

    feats = {
        'text': {
            'text': {
                'key': 'text'
            }
        }
        # ,
        # 'user': {
        #     'user': {
        #         'key': 'user.screen_name'
        #     }
        # }
    }

    vwf = VowpalFormatter(features=feats)

    kfk = KafkaModelConnector(
        group_id="activelearner-model-%s" % topic_id,
        unlabeled_consumer_topic="unique_tweets",
        labeled_consumer_topic="labeled-%s" % topic_id,
        label_request_producer_topic="label_request-%s" % topic_id
    )

    active_learner = ActiveLearnerModel(
        connector=kfk,
        mapper=vwf,
        autostart=False
    )

    active_learner.listen()
