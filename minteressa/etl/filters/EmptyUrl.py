import re
import json
import sys

from minteressa.etl.EtlProcessor import EtlProcessor
from minteressa.metrics.GraphiteClient import GraphiteClient


class EmptyUrl(EtlProcessor):
    """A class that acts over the raw tweets collected from the twitter stream
       in order to detect whether the tweet is duplicate, near-duplicate or
       nothing at all"""

    punct = re.compile(r"(es|en)", re.IGNORECASE)

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        EtlProcessor.__init__(self, connector=connector, autostart=autostart)
        self.graphite = GraphiteClient(["etl.url.discard", "etl.url.ok", "etl.url.error"])
        if autostart:
            self.listen()

    def listen(self):
        for msg in self.connector.listen():
            try:
                tweet = json.loads(msg.value())
                if len(tweet['entities']['urls']) > 0:
                    self.connector.send(msg.value())
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": self.connector.producer_topic
                        })
                    )
                    self.graphite.batch("etl.url.ok", 1)
                else:
                    self.graphite.batch("etl.url.discard", 1)

            except KeyError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "discard"
                    })
                )
                self.graphite.batch("etl.url.error", 1)
                continue
            except ValueError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "discard"
                    })
                )
                self.graphite.batch("etl.url.error", 1)
                continue
