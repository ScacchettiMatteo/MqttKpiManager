import time
from model.MQTT.mqtt_telemetry_collector import MqttTelemetryCollector
from utils.SenML.SenML_Pack import SenMLPack

topic = "/iot/user/271968@studenti.unimore.it/fum/hz_lab_mn/plcs/plc_ts17/sensors/+/auxiliaries/telemetry"
topic2 = "/iot/user/271968@studenti.unimore.it/fum/hz_lab_mn/plcs/plc_ts17/states/+/object_present/telemetry"
pub_topic = "/iot/user/271678@studenti.unimore.it/fum/hz_lab_mn/plcs/plc_ts17/states/cycle/object_present/control"

collector = MqttTelemetryCollector()
collector.init()
collector.subscribe_topic(topic)
collector.subscribe_topic(topic2)

for i in range(1000):
    pack = SenMLPack()
    pack.insert_senml_record(1, "TEST", time.time(), i, "u")
    collector.publish_senml_pack(pub_topic, pack)
    time.sleep(5)

collector.stop()
