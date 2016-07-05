

class EtlProcessor:
    """Base class that connects to a source, performs some
    operations and outputs the result"""

    connector = None

    def __init__(
        self,
        connector=None,
        autostart=True
    ):
        self.connector = connector
        if self.connector is None:
            raise ValueError("Need a valid connector to input and output data")
        else:
            if autostart:
                self.connector.connect()

    def listen(self):
        for msg in self.connector.consumer:
            print(msg)
