from socket import socket
import time


class GraphiteClient(object):
    def __init__(self, topics):
        self.socket = socket()
        self.socket.connect(('graphite', 2003))
        self.timestamp = {}
        self.count = {}
        self.topics = topics
        timestamp = str(int(time.time()))
        for topic in topics:
            self.timestamp[topic] = timestamp
            self.count[topic] = 0
        self.updated = timestamp

    def batch(self, topic, value):
        timestamp = str(int(time.time()))
        # print("Batched message %s" % timestamp)
        if self.timestamp[topic] == timestamp:
            self.count[topic] += value
        else:
            self.updated = timestamp
            for topic in self.topics:
                self.send(topic, self.timestamp[topic], self.count[topic])
                self.timestamp[topic] = timestamp
                self.count[topic] = 0

    def send(self, topic, timestamp, value):
        print("%s %s %s") % (topic, timestamp, value)
        self.socket.sendall("%s %s %s\n" % (topic, value, timestamp))
