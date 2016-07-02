from minteressa.etl.connectors.KafkaConnector import KafkaConnector
from minteressa.etl.filters.NearDuplicate import NearDuplicate

if __name__ == '__main__':
    kfk = KafkaConnector(
    	group_id="es_filter",
        consumer_topic="es_tweets",
        producer_topic="unique_tweets"
    )

    nd = NearDuplicate(connector=kfk, lang="es")