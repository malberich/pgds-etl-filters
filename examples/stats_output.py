from minteressa.etl.connectors.KafkaConnector import KafkaConnector
from minteressa.etl.filters.Lang import Lang

if __name__ == '__main__':
    kfk = KafkaConnector(
    	group_id='stats_output',
        consumer_topic="minteressa_stats",
        producer_topic=None
    )

    kfk.connect()

    for msg in kfk.consumer:
    	print(msg.value)