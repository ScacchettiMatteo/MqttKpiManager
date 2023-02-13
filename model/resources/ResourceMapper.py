import json
import logging
import yaml
from error.configuration_file_error import ConfigurationFileError
from model.resources.ResourceModel import ResourceModel


class ResourceMapper:
    _STR_CONFIG_FILE = '../../../config/resources/resources_configuration.yaml'

    def __init__(self, config_object=None, config_file_path=None):
        self._resources_dict = {}

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
                with open(self._STR_CONFIG_FILE, 'r') as file:
                    self._mapper = yaml.safe_load(file)
            except Exception as e:
                logging.error(str(e))
                raise ConfigurationFileError("Error while reading configuration") from None

        try:
            for key in self._mapper["resources"]:
                self._resources_dict[key] = ResourceModel.object_mapping(self._mapper["resources"][key])
        except Exception as e:
            logging.error(str(e))
            raise ConfigurationFileError("Error while parsing configuration data") from None

    def get_resource_dict(self):
        return self._resources_dict

    def set_resource_dict(self, resource_dict):
        self._resources_dict = resource_dict
