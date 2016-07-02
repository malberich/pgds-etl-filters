from minteressa.etl.connectors.KafkaConnector import KafkaConnector
from minteressa.etl.filters.EmptyUrl import EmptyUrl

if __name__ == '__main__':
    kfk = KafkaConnector(
        group_id="url_filter",
        consumer_topic="raw_tweets",
        producer_topic="url_tweets"
    )

    EmptyUrl(connector=kfk)
