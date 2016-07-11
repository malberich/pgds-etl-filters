import re
import json
from functools32 import lru_cache

from minteressa.etl.connectors.KafkaConnector import KafkaConnector
from minteressa.etl.mappers.Featurer import Featurer

if __name__ == '__main__':
    kfk = KafkaConnector(
    	group_id="tweet_featurer",
        consumer_topic="unique_tweets",
        producer_topic="featured_tweets"
    )

    featurer = Featurer()
