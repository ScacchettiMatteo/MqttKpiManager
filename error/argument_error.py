class ArgumentError(Exception):
    """
    Class that implements an argument error.
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return str(self.message)
