import re
import os
import json
import logging
import cPickle as pickle

from functools32 import lru_cache
from datasketch import MinHash, MinHashLSH
from minteressa.etl.EtlProcessor import EtlProcessor


class NearDuplicate(EtlProcessor):
    """A class that acts over the raw tweets collected from the twitter stream
       in order to detect whether the tweet is duplicate, near-duplicate or
       nothing at all"""

    punct = re.compile(r"[\.,;:]\\xe2", re.IGNORECASE)

    langs = {
        "es": "spanish",
        "en": "english"
    }

    process_count = 0

    def __init__(
        self,
        connector=None,
        lang='en',
        threshold=0.8,
        permutations=90,
        autostart=True
    ):
        self.permutations = permutations
        self.threshold = threshold
        self.lang = lang
        self.connector = None
        self.lsh = None

        EtlProcessor.__init__(self, connector=connector, autostart=autostart)
        if autostart:
            self.load()
            self.listen()

    def listen(self):
        """Performs a model check on whether the current tweet
        resembles at least to a 80% level as other previous tweets"""
        for msg in self.connector.listen():
            tweet = json.loads(msg.value())
            try:
                if self.is_unique(tweet):
                    self.connector.send(
                        msg.value()
                    )
                    self.connector.log(
                        json.dumps({
                            "id_str": tweet['id_str'],
                            "source": self.connector.consumer_topic,
                            "dest": self.connector.producer_topic
                        })
                    )
            except ValueError:
                self.connector.send(
                    json.dumps({
                        "id_str": tweet['id_str'],
                        "source": self.connector.consumer_topic,
                        "dest": "error"
                    })
                )
                continue
            finally:
                self.process_count += 1
                if self.process_count % 1000 == 0:
                    self.save()

    def load(self):
        """Loads the stored model data from previous runs"""
        if os.path.isfile('./minhash-%s-%.2f.pkl' % (self.lang, self.threshold)):
            self.lsh = pickle.load(
                open(
                    './minhash-%s--%d-%.2f.pkl' % (
                        self.lang,
                        self.permutations,
                        self.threshold
                    ),
                    'rb'
                )
            )
        else:
            self.lsh = MinHashLSH(
                threshold=self.threshold,
                num_perm=self.permutations
            )

    def save(self):
        """Stores the currently processed data for this model"""
        pickle.dump(
            self.lsh,
            open(
                './minhash-%s--%d-%.2f.pkl' % (
                    self.lang,
                    self.permutations,
                    self.threshold
                ),
                'wb+'
            )
        )

    def replace_urls(self, tweet):
        """Convenience function that replaces the compressed URLs by
        their expanded counterparts, in order to treat the same real URL
        as it is (and not obfuscating the same URL in diferent tweets by
        a different t.co link)"""
        removed_characters = 0
        if 'entities' in tweet and 'urls' in tweet['entities']:
            for url in tweet['entities']['urls']:
                tweet['text'] = tweet['text'][:(url['indices'][0] - removed_characters - 1)] + \
                    tweet['text'][(url['indices'][1] - removed_characters - 1):]
                removed_characters += url['indices'][1] - url['indices'][0]
            for url in tweet['entities']['urls']:
                tweet['text'] += ' ' + url['expanded_url']
        return tweet

    @lru_cache(maxsize=1e06)
    def minhash_tweet(self, tweet_text):
        """Minhashing operation that allows for a caching of up to
        1M tweets in order to speed up the checking procedure when it's
        the same tweet text"""
        tweet_hash = MinHash(num_perm=self.permutations)
        for word in tweet_text.split():
            tweet_hash.update(
                self.punct.sub(
                    "",
                    word.encode('utf8')
                )
            )
        return tweet_hash

    def is_unique(self, tweet):
        """Core method to check whether this tweet resembles enough to other previous
        tweets to label it as unique or near-duplicate"""
        is_unique_tweet = False
        urlfied_tweet = self.replace_urls(tweet)
        mht = self.minhash_tweet(
            urlfied_tweet['text']
        )
        if 'minteressa' not in tweet:
            tweet['minteressa'] = {}
        if self.lsh.is_empty() is not True:
            similars = self.lsh.query(mht)
            if len(similars) == 0:
                # It's a unique tweet
                try:
                    self.lsh.insert(
                        tweet['id_str'],
                        mht
                    )
                    is_unique_tweet = True
                except ValueError:
                    logging.error(ValueError)
            else:
                # nondupe
                for tweet_idx in similars:
                    if 'nearduplicates' not in tweet['minteressa']:
                        tweet['minteressa']['nearduplicates'] = 0

                tweet['minteressa']['nearduplicates'] += 1
        else:
            is_unique_tweet = True
            self.lsh.insert(
                tweet['id_str'],
                mht
            )
        return is_unique_tweet
