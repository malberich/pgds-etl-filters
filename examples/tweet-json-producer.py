from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import time
import json
import string
import gzip
import re
import sys

from minteressa.etl.connectors.KafkaConnector import KafkaConnector

def get_tweet(filename):
    """Simple generator to get a file full of JSON stringified objects
    and getting them converted into dictionaries to be able to use
    them into python code"""
    with gzip.open(filename, "r") as tweetfile:
            for row in tweetfile:
                yield json.loads(row)

def delivery_callback (self, err, msg):
        if err:
            print('%% Message failed delivery: %s\n' % err)
        else:
            print('%% Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))


if __name__ == '__main__':
    kfk = Producer({
        'bootstrap.servers': "kafka:2181",
        'group.id': "json_producer"
    })
    time.sleep(10)

    def delivery_callback (err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))

    for tweet in get_tweet('examples/tweets-200k.txt.gz'):
        # if len(tweet['entities']['urls']) > 0 and \
        #         any(tweet['lang'] in l for l in ['es', 'en']):
        try:
            print("%s: %s" % (tweet['user']['screen_name'], tweet['text']))
            kfk.produce(
                "raw_tweets",
                json.dumps(tweet),
                callback=delivery_callback
            )
            kfk.poll(0)
            kfk.flush()
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(kfk))


