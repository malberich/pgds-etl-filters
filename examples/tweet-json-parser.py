import json
import string
import gzip
import re
import argparse
from unidecode import unidecode


def get_tweet(filename):
    """Simple generator to get a file full of JSON stringified objects
    and getting them converted into dictionaries to be able to use
    them into python code"""
    with gzip.open(filename, "r") as tweetfile:
            for row in tweetfile:
                yield json.loads(row)

def replace_urls(tweet):
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

trailing_url = re.compile(r'https?[^\s]*$')
tco_url = re.compile(r'https:?[\/a-zA-Z\.]*')

def rmurls(tweet):
    """Convenience function that replaces the compressed URLs by
    their expanded counterparts, in order to treat the same real URL
    as it is (and not obfuscating the same URL in diferent tweets by
    a different t.co link)"""
    clean_tweet = re.sub(tco_url, '', tweet['text'])
    # removed_characters = 0
    # clean_tweet = ''
    # if 'entities' in tweet and 'urls' in tweet['entities']:
    #     curr_pos = 0
    #     for url in tweet['entities']['urls']:
    #         # if url['indices'][0] < curr_pos:
    #         clean_tweet += tweet['text'][curr_pos:(url['indices'][0]-1)]
    #         # else:
    #         #     clean_tweet += tweet['text'][curr_pos:(url['indices'][0]-1)]
    #         curr_pos = url['indices'][1]
    #     clean_tweet += tweet['text'][curr_pos:len(tweet['text'])]
    # clean_tweet = re.sub(trailing_url, '', clean_tweet)
    tweet['text'] = clean_tweet
    return tweet

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Extract tweets content and prepare for Vowpal Wabbit format.')
    parser.add_argument('--rmurl', action="store_true", help="Remove URLs from the tweet")
    parser.add_argument('--replaceurl', action="store_true", help="Replace t.co URLs by their expanded counterpart")

    cli_args = parser.parse_args()

    topics = {
        'cryptocoin': {
            'weight': 0.001,
            're': r'(btc|bitcoin|ethereum|fintech|coinbase|bitcoin|kraken|blockchain|cryptocurren|ripple|dodgecoin|cryptocoin)'
        },
        'isis': {
            'weight': 0.1,
            're': r'(orlando|shooting|gun|war|terrorism|isis|islamic state)'
        }
    }


    exclude = set(string.punctuation)

    twitter_abbrev=re.compile('(^\s)(rt|ht)[\s:]', re.IGNORECASE)

    for topic in topics:
        print(chr(27) + "[2J")
        print(
            "\033[2;1fStarting %s" % topic)
        # out_json = open("%s-classified.txt" % topic, 'w+')
        out_vw = open("%s-classified.vw" % topic, 'w+')
        topicre = re.compile(topics[topic]['re'])
        punct = re.compile("[" + re.escape(string.punctuation) + "\|\n]")
        counters = {
            'selected': 0,
            'unselected': 0
        }
        for tweet in get_tweet('tweets-200k.txt.gz'):
            if len(tweet['entities']['urls']) > 0 and \
                    any(tweet['lang'] in l for l in ['es', 'en']):

                if cli_args.replaceurl is True:
                    tweet = replace_urls(tweet)
                if cli_args.rmurl is True:
                    tweet = rmurls(tweet)

                tweet_text = unidecode(tweet['text'].lower())
                tweet_text = ''.join(
                    re.sub(
                        punct,
                        ' ',
                        ch
                    ) for ch in tweet_text
                )
                tweet_text = re.sub(twitter_abbrev, ' ', tweet_text)
                tweet_text = re.sub(r'\s\s+', ' ', tweet_text)
                tweet_text = re.sub(r'^\s+', '', tweet_text)

                if topicre.search(
                    tweet['text'],
                    re.IGNORECASE | re.MULTILINE
                ) is not None:
                    tweet['minteressa'] = {
                        'selected': 1,
                        'weight': 1,
                        'text': tweet_text
                    }
                    counters['selected'] += 1
                else:
                    tweet['minteressa'] = {
                        'selected': -1,
                        'weight': topics[topic]['weight'],
                        'text': tweet_text
                    }
                    counters['unselected'] += 1
                # print(tweet['text'])
                # out_json.write(json.dumps(tweet) + "\n")
                out_vw.write(
                    u"%s %.3f %s|text %s\n" % (
                        tweet['minteressa']['selected'],
                        tweet['minteressa']['weight'],
                        't' + tweet['id_str'],
                        tweet['minteressa']['text'],
                    )
                )
                print(
                    "\033[3;10f %d / %d" % (
                        counters['selected'],
                        counters['unselected']
                    )
                )
        out_vw.close()
        # out_json.close()

