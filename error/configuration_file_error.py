class ConfigurationFileError(Exception):
    """
    Class that implements a configuration file error.
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return str(self.message)
