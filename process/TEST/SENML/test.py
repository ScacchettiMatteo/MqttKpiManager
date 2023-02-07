import time
from utils.SenML.SenML_Pack import SenMLPack

pack = SenMLPack()
pack.insert_senml_record(1, "TEST", time.time(), "u", 1)
#print(pack.senml_pack_toString())
pack.string_to_senml_pack(pack.senml_pack_toString())
#print(pack.senml_pack_toString())
