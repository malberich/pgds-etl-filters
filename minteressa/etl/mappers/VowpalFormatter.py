import collections
import string


class VowpalFormatter():
    """Takes raw unique tweets and extracts their features"""

    """features is a dictionary pointing to the positions of the features
    inside of the json of the tweet"""
    features = {}
    label_feature = None

    punct = set(string.punctuation)

    def __init__(
        self,
        features={},
        label_feature=None
    ):
        self.features = features
        self.punct.add("\n")


    def _flatten(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def map(self, tweet, labeled=False):
        vw_string = u""
        flat_tweet = self._flatten(tweet)
        # print(flat_tweet)
        for ns in self.features:
            ns_string = ""
            for feature in self.features[ns]:
                try:
                    float(flat_tweet[self.features[ns][feature]['key']])
                    ns_string += u" %s:%s" % (
                        feature,
                        flat_tweet[self.features[ns][feature]['key']]
                    )
                except ValueError:
                    ns_string += u"%s " % (
                        ''.join(
                            ch for ch in
                            flat_tweet[self.features[ns][feature]['key']]
                            .encode('ascii', 'ignore')
                            .decode('ascii')
                            if ch not in self.punct
                        )
                    )
                    break
            ns_string = u"|%s %s" % (ns, ns_string)
            vw_string += ns_string

        if labeled is True:
            vw_string = (u"%s %s" % (tweet[self.label_feature], tweet['id_str'])) + vw_string
        return vw_string
