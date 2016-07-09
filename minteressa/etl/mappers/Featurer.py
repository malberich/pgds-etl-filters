import re
import json

from minteressa.etl.EtlProcessor import EtlProcessor


class Featurer(EtlProcessor):
    """A class that acts over the raw tweets collected from the twitter stream
       in order to detect whether the tweet is duplicate, near-duplicate or
       nothing at all"""

    punct = re.compile(r"(es|en)", re.IGNORECASE)
    langs = {
        "es": "spanish",
        "en": "english"
    }

    store = None

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        super(Featurer, self) \
            .__init__(connector=connector, autostart=autostart)

        if autostart is True:
            self.listen()

    def listen(self):
        for msg in self.connector.listen():
            try:
                tweet = json.loads(msg.value())
                tweet = self.extract_features(tweet)
                print(
                    "unique tweet: %s" % tweet['text']
                )
                self.connector.send(json.dumps(tweet))
            except ValueError:
                pass

    def extract_features(self, tweet):
        return tweet
