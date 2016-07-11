import re
import time
import json
import gzip

from vowpalwabbit import pyvw

from minteressa.etl.mappers.VowpalFormatter import VowpalFormatter


def get_tweet(filename):
    """Simple generator to get a file full of JSON stringified objects
    and getting them converted into dictionaries to be able to use
    them into python code"""
    with gzip.open(filename, "r") as tweetfile:
        for row in tweetfile:
            yield json.loads(row)

def get_vw(filename):
    """Simple generator to get a file full of JSON stringified objects
    and getting them converted into dictionaries to be able to use
    them into python code"""
    with open(filename, "r") as tweetfile:
        for row in tweetfile:
            yield row

if __name__ == '__main__':



    # feats = {
    #     '_weight':
    #     'text': {
    #         'user': {
    #             'key': 'user.screen_name'
    #         },
    #         'created_at': {
    #             'key': 'created_at'
    #         }
    #         # ,
    #         # 'text': {
    #         #     'key': 'text'
    #         # }
    #     }
    # }
    # vwf = VowpalFormatter(features=feats)
    # counter = 0

    topic = 'cryptocoin'

    vw_args = {
        'quiet': True,
        'passes': 10,
        'cache': True,
        'f':  "%s-predictor.vw" % topic,
        'k': True,
        'ngram': 2,
        'skips': 2,
        'ftrl': True,
        'decay_learning_rate': 0.99,
        'r': "%s-predictions.txt" % topic,
        # 'progressive_validation': "%s-validations.txt" % topic,
        'loss_function': 'hinge'
    }

    vw = pyvw.vw(**vw_args)

    for tweet in get_vw('%s-classified.train.vw' % topic):
        if (len(tweet) < 3):
            continue

        if tweet[:1] == '0':
            tweet = '-1' + tweet[1:]

        ex = vw.example(tweet)
        vw.learn(ex)
        # print(vw.predict(ex))
        # print(ex)
        print("%s" % (re.sub(r'\n', '', tweet)))
        # out.write(features + "\n")
        # counter += 1
        # if counter % 10000 == 0:
        #     print(counter)
    print("STARTING TEST")
    time.sleep(10)
    for tweet in get_vw('%s-classified.test.vw' % topic):
        pred = vw.predict(ex)
        if pred > 0.:
            print("%s -> %.1f" % (re.sub(r'\n', '', tweet), vw.predict(ex)))


    # out.close()
    # al = ActiveLearner(connector=kfk)
