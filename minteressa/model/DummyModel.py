import json

from minteressa.etl.EtlProcessor import EtlProcessor


class DummyModel(EtlProcessor):
    """A class that acts over the raw tweets collected from the
       twitter stream in order to detect whether the tweet may be
       relevant or not for the user"""

    model = None

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        print("Connecting to Kafka...")
        EtlProcessor.__init__(self, connector=connector, autostart=autostart)

        if autostart:
            self.listen()

    def listen(self):
        for msg in self.connector.consumer:
            if msg.key == self.connector.unlabeled_consumer_topic:
                self.connector.send(
                    self.label_request_producer,
                    json.dumps(msg.value)
                )
            else:
                # msg.key = self.labeled_consumer
                # That's a tweet that has been labeled by the user
                # Should feed the model but
                # given that this model is dummy, it does nothing
                self.partial_fit(
                    msg.value,
                    msg.value['minteressa']['selected']
                )
                print(
                    "Tweet %s: User has chosen %s" % (
                        msg.value['id_str'],
                        msg.value['minteressa']['selected']
                    )
                )
                continue

    def predict(self, X):
        """Following the sklearn naming convention, this method should
        get an unlabeled itemd and perform a prediction over it"""
        pass

    def fit(self, X, y):
        """Following the sklearn naming convention, this method should
        get all the available data and try to tune the selected model"""
        pass

    def partial_fit(self, X, y):
        """Following the sklearn naming convention, this method should
        incrementally fit the model as there come new """
        pass
