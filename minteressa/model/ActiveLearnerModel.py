import re
import os.path
import json
import time

from vowpalwabbit import pyvw
# >>> vw = pyvw.vw(quiet=True)
# >>> ex = vw.example('1 | a b c')
# >>> vw.learn(ex)
# >>> vw.predict(ex)

import threading
import subprocess

from minteressa.etl.EtlProcessor import EtlProcessor

import socket

def recvall(s, n):
    buf=s.recv(n)
    ret=len(buf)
    while ret>0 and len(buf)<n:
        if buf[-1]=='\n': break
        tmp=s.recv(n)
        ret=len(tmp)
        buf=buf+tmp
    return buf

class ActiveLearnerModel(EtlProcessor):
    """A class that acts over the raw tweets collected from the
       twitter stream in order to detect whether the tweet may be
       relevant or not for the user"""

    vw_thread = None

    features = {
        "basic": {
            'user': {
                'key': 'user.screen_name'
            },
            'created_at': {
                'key': 'created_at'
            }
        }
    }
    connector = None
    mapper = None
    vw_args = {
        "loss_function": "hinge",
        "port": 26542,
        "daemon": True,

        "active": True,
        "mellowness": 0.01,

        "decay_learning_reate": .95,
        "learning_rate": -0.001,
        "log_multi": 16,
        "l2": 1e-6,

        "ngram": 2,
        "skip": 2,

        "save_resume": False,
        "no_stdin": True,

    }
    vw = None
    hostname = "localhost"
    port = 26542

    s = None

    def __init__(
        self,
        hostname="127.0.0.1",
        port=26542,
        features=None,
        mapper=None,
        connector=None,
        autostart=True
    ):
        self.features = None
        if features is not None:
            self.features = features

        self.mapper = None
        if mapper is not None:
            self.mapper = mapper

        self.connector = None
        if connector is not None:
            self.connector = connector

        self.port = None
        if port is not None:
            self.port = port

        self.hostname = None
        if hostname is not None:
            self.hostname = hostname

        EtlProcessor.__init__(self, connector=connector, autostart=autostart)

        if autostart is True:
            self.listen()

    def netcat(self, content, response=True):
        data = ""
        self.s.sendall(content)
        if response is True:
            data = recvall(self.s, 1024)
        return data

    def listen(self):
        self.start_vw()
        print("Initializing Vowpal Wabbit")
        time.sleep(1)
        if self.s is None:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect((self.hostname, self.port))

        print("Done. Listening")

        print(
            "Seeding vowpal with train data from '%s'..." %
            self.connector.labeled_consumer_topic
        )
        for msg in self.connector.load_labeled():
            if self.connector.connector_type == 'QUEUE':
                row = json.loads(msg.value())
                if 'stop_train' in row.keys():
                    break
                if self.mapper is not None and \
                    hasattr(self.mapper, 'map') and \
                        hasattr(self.mapper.map, '__call__'):
                    row = self.mapper.map(row)
            else:
                row = msg
            response = self.netcat(row)

        print(
            "VW trained, now predicting CV data from '%s'..." %
            self.connector.unlabeled_consumer_topic
        )
        re_class = re.compile(r'^-?1')
        counter = 0

        for msg in self.connector.load_unlabeled():
            if self.connector.connector_type == 'QUEUE':
                row = json.loads(msg.value())
                if 'stop_train' in row.keys():
                    break
                if self.mapper is not None and \
                    hasattr(self.mapper, 'map') and \
                        hasattr(self.mapper.map, '__call__'):
                    row = self.mapper.map(row)
            else:
                row = msg

            line_part = msg.split('|')
            label_elements = line_part[0].split()

            line_part[0] = label_elements[-1]
            msg = ' ' + '|'.join(line_part)

            # print("Unlabeled text: %s" % msg)
            response = self.netcat(msg)
            try:
                label_predict, tag, importance = response.split()
                # print("Importance: %.2f" % float(importance))
                if float(importance) >= 1.:
                    label = self.connector.request_label(msg, float(label_predict))
                    line_part[0] = "%d %.3f %s" % (label, float(importance), tag)
                    self.netcat('|'.join(line_part))
                    # print("Should ask for tweet %s " % msg)
                    # raise ValueError("Should ask for tweet %s " % msg)
                    self.connector.log(json.dumps({
                        "id_str": tag.replace('t', ''),
                        "source": self.connector.unlabeled_consumer_topic,
                        "dest": "label_request"
                    }))
                else:
                    self.connector.log(json.dumps({
                        "id_str": tag.replace('t', ''),
                        "source": self.connector.unlabeled_consumer_topic,
                        "dest": "unlabeled"
                    }))
            except ValueError:
                pass
            counter += 1
            if counter % 10 == 0:
                print(self.netcat('save_test_model.vw\r\n', response=False))


            # try:
            #     print(features)
            #     self.netcat(features)
            #     ex = self.vw.example(features)

            #     response = self.vw.learn(ex)


            #     print  self.vw.get_updated_prediction() #<-- usually 0.0
            #     print  self.vw.get_simplelabel_prediction() #<-- the same for every prediction?

            #     print(response)
            #     label, tag, importance = response.split()
            # except ValueError:
            #     print("Error on response: %s" % response)
            #     # self.connector.log(json.dumps({
            #     #     "id_str": tweet['id_str'],
            #     #     "source": self.connector.consumer_topic,
            #     #     "dest": "error"
            #     # }))
            #     continue

            # if importance >= 1.:
            #     print("Should ask for tweet %s " % features)
            #     self.connector.log(json.dumps({
            #         "id_str": tweet['id_str'],
            #         "source": self.connector.unlabeled_consumer_topic,
            #         "dest": "label_request"
            #     }))
            # else:
            #     self.connector.log(json.dumps({
            #         "id_str": tweet['id_str'],
            #         "source": self.connector.unlabeled_consumer_topic,
            #         "dest": "unlabeled"
            #     }))

    def fit(self):
        pass

    def predict(self, tweet):
        pass

    def start_vw(self):
        print("Starting VW in the background")
        command = ' '.join(
            [
                "/usr/local/bin/vw",
                "--daemon",
                "--save_resume",
                "--pid_file",
                "./vw.pid",
                "--mellowness",
                "0.005",
                "-f",
                "final_regressor.vw",
                "--readable_model",
                "readable_final_regressor.vw",
                "--save_per_pass",
                "--port",
                str(self.port),
                "--loss_function",
                "hinge",
                "--quiet",
                "--no_stdin",
                "--active",
                "&"
            ]
        )

        print(command)

        # self.vw = subprocess.call(command)
        # port="26542",
        # daemon=True,
        # quiet=True,
        # no_stdin=True,
        # save_resume=True,
        # pid_file="./vw.pid",
        # loss_function="hinge",
        # mellowness="0.01",
        # active=True

        os.system(command)
        # if os.path.isfile("./vw.pid") is False:
        #     raise IOError("Could not start Vowpal Wabbit")
        # else:
        #     print("Started")
        # pass

    def stop_vw(self):
        os.system("kill `cat vw.pid`")
        # self.vw.finish()
        # with open("./vw.pid") as f:
        #     pid = f.read()
        # f.close()
        # call([
        #     "kill",
        #     pid.replace("\n", "")
        # ])

    def save_model(self):
        self.vw.save_model(self.get_model_filename())
        pass

    def get_model_filename(self):
        return "%s.model" % self.connector.unlabeled_consumer_topic

