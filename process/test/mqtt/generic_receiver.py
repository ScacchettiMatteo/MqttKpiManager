import time

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("Client " + str("test") + " connected with result code " + str(rc))


def on_disconnect(client, userdata, rc):
    print("Client " + str("test") + " disconnected with result code " + str(rc))


def on_message(client, userdata, message):
    string_payload = str(message.payload.decode("utf-8"))
    topic = message.topic
    print(f"Received IoT Message: Topic: {topic} Payload: {string_payload}")


mqtt_client = mqtt.Client("test", clean_session=False)
mqtt_client.username_pw_set("unimore-fum-lab", "wxugudolcbjttoke")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect
print("Connecting to " + "155.185.4.4" + " port: " + str(7883))
mqtt_client.connect("155.185.4.4", 7883)
mqtt_client.loop_start()
mqtt_client.subscribe("#")
time.sleep(100000)
