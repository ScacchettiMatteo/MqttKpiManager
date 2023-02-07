import json
import logging
from error.mqtt.senml_format_error import SenmlFormatError
from utils.SenML.SenML_Record import SenMLRecord


class SenMLPack:

    def __init__(self):
        self._senMLPack = []

    def __int__(self, senMLPack):
        self._senMLPack = senMLPack

    def get_senml_pack(self):
        return self._senMLPack

    def insert_senml_record(self, version=None, name=None, time=None, value=None, unit=None):
        record = SenMLRecord()
        if version is not None:
            record.set_bver(version)
        if name is not None:
            record.set_bn(name)
        if time is not None:
            record.set_bt(time)
        if value is not None:
            record.set_v(value)
        if unit is not None:
            record.set_u(unit)
        self._senMLPack.append(record)

    def senml_pack_toString(self):
        return json.dumps(self._senMLPack,
                          default=lambda o: dict((key, value) for key, value in o.__dict__.items() if value),
                          sort_keys=False,
                          allow_nan=False)

    def string_to_senml_pack(self, payload):
        try:
            self._senMLPack = json.loads(payload)
            if len(self._senMLPack) <= 0:
                logging.warning("Senml pack is empty")
        except Exception as e:
            logging.error(str(e))
            raise SenmlFormatError("Payload format is not support -- required senml") from None
