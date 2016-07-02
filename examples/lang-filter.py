from minteressa.etl.connectors.KafkaConnector import KafkaConnector
from minteressa.etl.filters.Lang import Lang

if __name__ == '__main__':
    kfk = KafkaConnector(
    	group_id="lang_filter",
        consumer_topic="raw_tweets"
    )

    lang_filter = Lang(
        connector=kfk,
        allowed_langs=["es", "en"]
	)