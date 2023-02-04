import json
import logging
import senml
from error.mqtt.senml_format_error import SenmlFormatError


class SenMLPack:

    def __init__(self):
        self._senMLPack = []

    def __int__(self, senMLPack):
        self._senMLPack = senMLPack

    def get_senml_pack(self):
        return senml.SenMLDocument(measurements=self._senMLPack)

    def insert_senml_record(self, name=None, time=None, value=None, unit=None):
        record = senml.SenMLMeasurement()
        if name is not None:
            record.name = name
        if time is not None:
            record.time = time
        if value is not None:
            record.value = value
        if unit is not None:
            record.unit = unit
        self._senMLPack.append(record)

    def senml_pack_toString(self):
        message = senml.SenMLDocument(measurements=self._senMLPack)
        return json.dumps(message.to_json())

    def string_to_senml_pack(self, payload):
        try:
            pack = senml.SenMLDocument.from_json(json.loads(payload))
            self._senMLPack = pack.measurements
            if len(pack.measurements) <= 0:
                logging.warning("Senml pack is empty")
        except Exception as e:
            logging.error(str(e))
            raise SenmlFormatError("Payload format is not support -- required senml") from None
