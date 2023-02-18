import json


class ResourceModel:
    def __init__(self, *args):
        self.uuid = None
        self.version = None
        self.device = None
        self.name = None
        self.unit = None
        self.topic = None
        self.qos = None
        self.retained = None
        self.frequency = None
        self.value = None

        if len(args) > 0 and isinstance(args[0], dict):
            vars(self).update(args[0])

    def get_uuid(self):
        return self.uuid

    def get_version(self):
        return self.version

    def get_device(self):
        return self.device

    def get_name(self):
        return self.name

    def get_unit(self):
        return self.unit

    def get_topic(self):
        return self.topic

    def get_qos(self):
        return self.qos

    def get_retained(self):
        return self.retained

    def get_frequency(self):
        return self.frequency

    def get_value(self):
        return self.value

    def set_uuid(self, uuid):
        self.uuid = uuid

    def set_version(self, version):
        self.version = version

    def set_device(self, device):
        self.device = device

    def set_name(self, name):
        self.name = name

    def set_unit(self, unit):
        self.unit = unit

    def set_topic(self, topic):
        self.topic = topic

    def set_qos(self, qos):
        self.qos = qos

    def set_retained(self, retained):
        self.retained = retained

    def set_frequency(self, frequency):
        self.frequency = frequency

    def set_value(self, value):
        self.value = value

    def __str__(self):
        return f'ResourceModel({self.uuid},{self.version},{self.name},{self.unit},{self.topic},{self.qos},{self.retained},{self.frequency},{self.value})'

    @staticmethod
    def object_mapping(dictionary):
        return json.loads(json.dumps(dictionary), object_hook=ResourceModel)
