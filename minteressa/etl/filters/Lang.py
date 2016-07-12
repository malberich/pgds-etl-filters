import re
import json
from functools32 import lru_cache
from minteressa.etl.EtlProcessor import EtlProcessor
from minteressa.metrics.GraphiteClient import GraphiteClient


class Lang(EtlProcessor):
    """A class that acts over the raw tweets collected from the twitter stream
       in order to detect whether the tweet is written into a the required
       language or not. If so, it gets forwared to the next stage. Otherwise
       it gets filtered out"""

    langs = {
        "es": "spanish",
        "en": "english"
    }

    def __init__(
        self,
        allowed_langs=['en', 'es'],
        connector=None,
        autostart=True
    ):
        self.allowed_langs = allowed_langs
        if self.allowed_langs is None:
            raise ValueError("Need at least a language to be selected")
        else:
            self.punct = re.compile(
                r"(" +
                '|'.join(self.allowed_langs) +
                ")",
                re.IGNORECASE
            )
        self.graphite = GraphiteClient(["etl.lang.es", "etl.lang.en", "etl.lang.discarded", "etl.lang.error"])
        EtlProcessor.__init__(self, connector=connector, autostart=autostart)
        if autostart is True:
            self.listen()

    def listen(self):
        for msg in self.connector.listen():
            try:
                tweet = json.loads(msg.value())
                if self.is_valid(tweet['lang']):
                    lang_topic = "%s_tweets" % tweet['lang']
                    self.connector.send(
                        msg.value(),
                        producer_topic="%s_tweets" % tweet['lang']
                    )
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": lang_topic
                        })
                    )
                    self.graphite.batch("lang.%s" % tweet['lang'], 1)
                else:
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": "discard"
                        })
                    )
                    self.graphite.batch("etl.lang.discard", 1)
                    
            except KeyError:
                print("Key error on tweet %s" % tweet['id_str'])
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "error"
                    })
                )
                self.graphite.batch("etl.lang.error", 1)
                continue
            except ValueError:
                print("Value error on tweet %s" % tweet['id_str'])
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "error"
                    })
                )
                self.graphite.batch("etl.lang.error", 1)
                continue

    @lru_cache(maxsize=200)
    def is_valid(self, lang):
        matched = self.punct.search(lang)
        return False if matched is None else True
