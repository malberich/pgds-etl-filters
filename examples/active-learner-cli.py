from minteressa.etl.connectors.VwModelConnector import VwModelConnector
from minteressa.model.ActiveLearnerModel import ActiveLearnerModel

import signal
import sys


if __name__ == '__main__':
    al = None

    def stop_vw(signal, frame):
        print('You pressed Ctrl+C!')
        al.stop_vw()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_vw)

    vwc = VwModelConnector(
        unlabeled_consumer_topic='isis-classified.test.vw',
        labeled_consumer_topic='isis-classified.train.vw',
        label_request_producer_topic=None,
        logging_topic=None,
        bootstrap_servers=None
    )

    al = ActiveLearnerModel(
        connector=vwc,
        autostart=False
    )

    al.listen()
    
