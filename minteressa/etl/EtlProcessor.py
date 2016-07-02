class EtlProcessor:
    """Base class that connects to a source, performs some
    operations and outputs the result"""

    connector = None

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        self.connector = connector \
            if connector is not None \
            else None

        if self.connector is None:
            raise ValueError("Need a valid connector to input and output data")

        if autostart is True:
            self.start()

    def start(self):
        self.connector.connect()

    def listen(self):
        for msg in self.connector.consumer:
            yield msg
