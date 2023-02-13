import logging
import yaml
from error.configuration_file_error import ConfigurationFileError


class ResourceMapper:
    def __init__(self, config_object=None, config_file_path=None):

        if config_object is not None:
            self._mapper = config_object
        elif config_file_path is not None:
            try:
                with open(config_file_path, 'r') as file:
                    self._mapper = yaml.safe_load(file)
            except Exception as e:
                logging.error(str(e))
                raise ConfigurationFileError("Error while reading configuration file") from None
        else:
            try:
                with open('../config/resources/resources_configuration.yaml', 'r') as file:
                    self._mapper = yaml.safe_load(file)
            except Exception as e:
                logging.error(str(e))
                raise ConfigurationFileError("Error while reading configuration") from None
        print(self._mapper)
