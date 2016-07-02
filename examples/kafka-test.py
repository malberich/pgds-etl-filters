from minteressa.etl.connectors.KafkaConnector import KafkaConnector

if __name__ == '__main__':
    kfk = KafkaConnector(
        group_id="kafka_test",
        consumer_topic="raw_tweets"
    )

    kfk.connect()
    kfk.listen()
