import time
from utils.SenML.SenML_Pack import SenMLPack

pack = SenMLPack()
pack.insert_senml_record(bver=1, bn="TEST", bt=time.time(), u="u", v=1)
pack.insert_senml_record(bver=1, bn="TEST", bt=time.time(), u="u", v=1)

for record in pack.get_senml_pack():
    print(record.toJSON())

print(pack.senml_pack_toString())
pack.string_to_senml_pack(pack.senml_pack_toString())
print(pack.senml_pack_toString())

for record in pack.get_senml_pack():
    print(record.toJSON())
