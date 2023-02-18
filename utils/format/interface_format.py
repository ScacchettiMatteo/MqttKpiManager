import abc


class IFormat(abc.ABC):

    @abc.abstractmethod
    def initialize(self):
        pass

    @abc.abstractmethod
    def to_format(self, resource):
        pass

    @abc.abstractmethod
    def from_format(self, senml_pack, resource):
        pass
