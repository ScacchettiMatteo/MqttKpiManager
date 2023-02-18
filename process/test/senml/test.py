import time
from utils.senml.SenML_Pack import SenMLPack
from utils.senml.SenML_Record import SenMLRecord

pack = SenMLPack("test", 10)
record = SenMLRecord()
record.set_bn("NAME:")
record.set_v(10)
record2 = SenMLRecord()
record2.set_n("RISORSA")
record2.set_v(10)

pack.insert_senml_record_object(record)
pack.insert_senml_record_object(record2)
payload = pack.senml_pack_to_json()

pack2 = SenMLPack()
pack2.json_to_senml_pack(payload)
print(isinstance(pack2, SenMLPack))
print(pack2.get_senml_pack())

for record in pack2.get_senml_pack():
    print(record.to_json())
