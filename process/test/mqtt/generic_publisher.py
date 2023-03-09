import time
import paho.mqtt.client as mqtt
from model.resources.resources_mapper import ResourcesMapper
from utils.format.jsonsenml.format_json_senml import FormatJsonSenML

resource = ResourcesMapper().get_resource("joint_current_consumptions")
resource.set_value([1, 1, 1, 1, 1, 1])
formatter = FormatJsonSenML()
pack = formatter.to_format(resource)
message = pack.to_json()
print(message)

mqtt_client = mqtt.Client("sender", clean_session=False)
mqtt_client.username_pw_set("unimore-fum-lab", "wxugudolcbjttoke")
print("Connecting to " + "192.168.1.174" + " port: " + str(1883))
mqtt_client.connect("155.185.4.4", 7883)
mqtt_client.loop_start()
for i in range(100):
    mqtt_client.publish("/iot/user/unimore-fum-lab/edge/systems/pnp_unimore_fum_lab/cobot/joints/sensors/current_consumptions/telemetry", message)
    time.sleep(5)
