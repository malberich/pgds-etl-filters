import re
import json
from functools32 import lru_cache
from minteressa.etl.EtlProcessor import EtlProcessor


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

        EtlProcessor.__init__(self, connector=connector, autostart=autostart)
        if autostart is True:
            self.listen()

    def listen(self):
        for msg in self.connector.consumer:
            try:
                tweet = msg.value
                if self.is_valid(tweet['lang']):
                    lang_topic = "%s_tweets" % tweet['lang']
                    self.connector.send(
                        json.dumps(msg.value),
                        producer_topic="%s_tweets" % tweet['lang']
                    )
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": lang_topic
                        })
                    )
                else:
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": "discard"
                        })
                    )
            except KeyError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "error"
                    })
                )
                continue
            except ValueError:
                self.connector.log(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "error"
                    })
                )
                continue

    @lru_cache(maxsize=200)
    def is_valid(self, lang):
        matched = self.punct.search(lang)
        return False if matched is None else True
