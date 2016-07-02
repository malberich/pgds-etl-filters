import re
import json
import logging
from functools32 import lru_cache

from minteressa.etl.EtlProcessor import EtlProcessor


class EmptyUrl(EtlProcessor):
    """A class that acts over the raw tweets collected from the twitter stream
       in order to detect whether the tweet is duplicate, near-duplicate or
       nothing at all"""

    punct = re.compile(r"(es|en)", re.IGNORECASE)
    store = None

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        EtlProcessor.__init__(self, connector=connector, autostart=autostart)


        if autostart is True:
            self.listen()

    def listen(self):
        print(
            "Listening to messages to consume from '%s'" %
            self.connector.consumer_topic
        )
        for msg in self.connector.consumer:
            try:
                tweet = msg.value
                if len(tweet['entities']['urls']) > 0:
                    self.connector.send(json.dumps(msg.value))
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": self.connector.producer_topic
                        })
                    )
                    print("Passing tweet %s" % tweet['id_str'])
                else:
                    print("Discarding tweet %s" % tweet['id_str'])

            except KeyError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "discard"
                    })
                )
                continue
            except ValueError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "discard"
                    })
                )
                continue
