import json
from error.mqtt.senml_format_error import SenmlFormatError
from utils.SenML.SenML_Record import SenMLRecord


class SenMLPack:

    def __init__(self, *args):
        self._senMLPack = []
        if len(args) > 0 and isinstance(args[0], dict):
            vars(self).update(args[0])

    def get_senml_pack(self):
        return self._senMLPack

    def set_senml_pack(self, senMLPack):
        self._senMLPack = senMLPack

    def insert_senml_record(self, bver=None, bn=None, bt=None, bu=None, bv=None, v=None, n=None, u=None, vb=None):
        record = SenMLRecord()
        if bver is not None:
            record.set_bver(bver)
        if bn is not None:
            record.set_bn(bn)
        if bt is not None:
            record.set_bt(bt)
        if bu is not None:
            record.set_bu(bu)
        if bv is not None:
            record.set_bv(bv)
        if v is not None:
            record.set_v(v)
        if n is not None:
            record.set_n(n)
        if u is not None:
            record.set_u(u)
        if vb is not None:
            record.set_vb(vb)
        self._senMLPack.append(record)

    def insert_senml_record_object(self, record):
        if isinstance(record, SenMLRecord):
            self._senMLPack.append(record)
        else:
            raise SenmlFormatError("Object format is not support -- required senml") from None

    def senml_pack_to_json(self):
        return json.dumps(self._senMLPack,
                          default=lambda o: dict((key, value) for key, value in o.__dict__.items() if value),
                          sort_keys=False,
                          allow_nan=False)

    def json_to_senml_pack(self, data):
        senml_pack = json.loads(data)
        self._senMLPack = []
        for senml_record in senml_pack:
            self._senMLPack.append(json.loads(str(senml_record).replace("'", "\""), object_hook=SenMLRecord))
