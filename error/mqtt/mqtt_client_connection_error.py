class MqttClientConnectionError(Exception):
    """
    Class that implements a mqtt client connection error.
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return str(self.message)
