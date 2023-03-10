import json
import logging
import time
import uuid
from threading import Thread
import paho.mqtt.client as mqtt
import yaml
from error.configuration_file_error import ConfigurationFileError
from error.mqtt.mqtt_client_connection_error import MqttClientConnectionError
from model.mqtt.mqtt_publisher_to_thingsboard import MqttPublisherToThingsboard
from model.resources.resource_model import ResourceModel
from model.resources.resources_mapper import ResourcesMapper
from utils.format.jsonsenml.format_json_senml import FormatJsonSenML
from utils.senml.SenML_Pack import SenMLPack


class MqttTelemetryCollector:
    _timer = 40
    _timer_test = 20
    _STR_DEVICE = "Kpi_Fum_Lab"
    _STR_DEVICE_TEST = "Kpi_Simulated_Lab"
    _STR_CLIENT_ID = "client_id"
    _STR_QOS = "qos"
    _STR_BROKER_ADDRESS = "broker_address"
    _STR_BROKER_PORT = "broker_port"
    _STR_MQTT_USERNAME = "mqtt_username"
    _STR_MQTT_PASSWORD = "mqtt_password"
    _STR_QOS_PUBLISH = "qos_publish"
    _STR_QOS_SUBSCRIBE = "qos_subscribe"
    _STR_BASE_TOPIC = "base_topic"
    _STR_TELEMETRY_TOPIC = "telemetry_topic"
    _STR_CONNECTION_TOPIC = "connection_topic"
    _STR_DISCONNECTION_TOPIC = "disconnection_topic"

    def __init__(self, config_object=None, config_file_path=None):

        if config_object is not None:
            self._mapper = config_object
        elif config_file_path is not None:
            try:
                with open(config_file_path, 'r') as file:
                    self._mapper = yaml.safe_load(file)
            except Exception as e:
                logging.error(str(e))
                raise ConfigurationFileError("Error while reading configuration file") from None
        else:
            try:
                with open('../config/mqtt/mqtt_telemetry_collector_config.yaml', 'r') as file:
                    self._mapper = yaml.safe_load(file)
            except Exception as e:
                logging.error(str(e))
                raise ConfigurationFileError("Error while reading configuration") from None

        if self._STR_CLIENT_ID in self._mapper and self._mapper[self._STR_CLIENT_ID] is not None:
            self._client_id = self._mapper[self._STR_CLIENT_ID]
        else:
            self._client_id = str(uuid.uuid4())

        if self._STR_BROKER_ADDRESS in self._mapper and self._mapper[self._STR_BROKER_ADDRESS] is not None:
            self._broker_address = self._mapper[self._STR_BROKER_ADDRESS]
        else:
            logging.warning("Broker address is not specified")
            self._broker_address = None

        if self._STR_BROKER_PORT in self._mapper and self._mapper[self._STR_BROKER_PORT] is not None:
            self._broker_port = self._mapper[self._STR_BROKER_PORT]
        else:
            logging.warning("Broker port is not specified")
            self._broker_port = None

        if self._STR_MQTT_USERNAME in self._mapper and self._mapper[self._STR_MQTT_USERNAME] is not None:
            self._mqtt_username = self._mapper[self._STR_MQTT_USERNAME]
        else:
            logging.warning("Broker username is not specified")
            self._mqtt_username = None

        if self._STR_MQTT_PASSWORD in self._mapper and self._mapper[self._STR_MQTT_PASSWORD] is not None:
            self._mqtt_password = self._mapper[self._STR_MQTT_PASSWORD]
        else:
            logging.warning("Broker password is not specified")
            self._mqtt_password = None

        if self._STR_QOS_PUBLISH in self._mapper and self._mapper[self._STR_QOS_PUBLISH] is not None:
            self._qos_publish = self._mapper[self._STR_QOS_PUBLISH]
        else:
            logging.warning("Qos publish is not specified")
            self._qos_publish = 0

        if self._STR_QOS_SUBSCRIBE in self._mapper and self._mapper[self._STR_QOS_SUBSCRIBE] is not None:
            self._qos_subscribe = self._mapper[self._STR_QOS_SUBSCRIBE]
        else:
            logging.warning("Qos subscribe is not specified")
            self._qos_subscribe = 0

        if self._STR_BASE_TOPIC in self._mapper and self._mapper[self._STR_BASE_TOPIC] is not None:
            self._mqtt_basic_topic = self._mapper[self._STR_BASE_TOPIC]
        else:
            logging.warning("Base topic is not specified")
            self._mqtt_basic_topic = "/iot/user/{0}".format(self._mqtt_username)

        if self._STR_TELEMETRY_TOPIC in self._mapper and self._mapper[self._STR_TELEMETRY_TOPIC] is not None:
            self._mqtt_telemetry_topic = self._mapper[self._STR_TELEMETRY_TOPIC]
        else:
            logging.warning("Telemetry topic is not specified")
            self._mqtt_telemetry_topic = "/telemetry"

        if self._STR_CONNECTION_TOPIC in self._mapper and self._mapper[self._STR_CONNECTION_TOPIC] is not None:
            self._connection_topic = self._mapper[self._STR_CONNECTION_TOPIC]
        else:
            self._action_topic = "/connect"

        if self._STR_DISCONNECTION_TOPIC in self._mapper and self._mapper[self._STR_DISCONNECTION_TOPIC] is not None:
            self._connection_topic = self._mapper[self._STR_DISCONNECTION_TOPIC]
        else:
            self._action_topic = "/disconnect"

        self._mqtt_client = None
        self._mqtt_publisher = MqttPublisherToThingsboard()
        self.format_json_senml = FormatJsonSenML()
        self.resource_mapper = ResourcesMapper()
        self._resources_dict = self.resource_mapper.get_resources()
        self._topics_dict = {}
        for resource in self._resources_dict:
            self._topics_dict[self._mqtt_basic_topic + self._resources_dict[resource].get_topic()] = resource

    def on_connect(self, client, userdata, flags, rc):
        print("Client " + str(self._client_id) + " connected with result code " + str(rc))
        self.start_subscribe()

    def on_disconnect(self, client, userdata, rc):
        print("Client " + str(self._client_id) + " disconnected with result code " + str(rc))

    def on_message(self, client, userdata, message):
        string_payload = str(message.payload.decode("utf-8"))
        topic = message.topic
        #print(f"Received IoT Message: Topic: {topic} Payload: {string_payload}")
        self.update_kpi(topic, string_payload)

    def init(self):
        try:
            self._mqtt_publisher.init()
            self._mqtt_client = mqtt.Client(self._client_id, clean_session=False)
            self._mqtt_client.on_connect = self.on_connect
            self._mqtt_client.on_message = self.on_message
            self._mqtt_client.on_disconnect = self.on_disconnect
            self._mqtt_client.username_pw_set(self._mqtt_username, self._mqtt_password)
            print("Connecting to " + self._broker_address + " port: " + str(self._broker_port))
            self._mqtt_client.connect(self._broker_address, self._broker_port)
        except Exception as e:
            logging.error(str(e))
            raise MqttClientConnectionError("Error during mqtt client connection") from None
        Thread(target=self.publish_message).start()
        self._mqtt_client.loop_forever()

    def set_client_id(self, client_id):
        self._client_id = client_id

    def set_broker_address(self, broker_address):
        self._broker_address = broker_address

    def set_broker_port(self, broker_port):
        self._broker_port = broker_port

    def set_mqtt_username(self, mqtt_username):
        self._mqtt_username = mqtt_username

    def set_mqtt_password(self, mqtt_password):
        self._mqtt_password = mqtt_password

    def start_subscribe(self):
        for topic in self._topics_dict:
            self.subscribe_topic(topic, self._resources_dict[self._topics_dict[topic]].get_qos())

    def subscribe_topic(self, topic, qos=None):
        if qos is None:
            qos = self._qos_subscribe
        self._mqtt_client.subscribe(topic, qos=qos)
        print(f"Subscribe to topic: {topic}")

    def update_kpi(self, topic, string_payload):
        try:
            resource_name = self._topics_dict[topic]
            senml_pack = SenMLPack()
            senml_pack.from_json(string_payload)
            resource = ResourceModel()
            self.format_json_senml.from_format(senml_pack, resource)
            self.update_data(resource_name, resource)
        except:
            logging.warning("Senml format wrong")

    def update_data(self, resource_name, resource):
        if self._resources_dict[resource_name].get_operation() == 'sum':
            self._resources_dict[resource_name].set_value(self._resources_dict[resource_name].get_value() + resource.get_value())
        elif self._resources_dict[resource_name].get_operation() == 'mean':
            self._resources_dict[resource_name].set_value((self._resources_dict[resource_name].get_value() + resource.get_value()) / 2)
        elif self._resources_dict[resource_name].get_operation() == 'assign':
            self._resources_dict[resource_name].set_value(resource.get_value())
        elif self._resources_dict[resource_name].get_operation() == 'double_mean':
            self._resources_dict[resource_name].set_value((self._resources_dict[resource_name].get_value() + (sum(resource.get_value()) / len(resource.get_value()))) / 2)

    def publish_message(self, retained=False):
        while True:
            time.sleep(self._timer)
            payload = self._resources_dict
            self._resources_dict = ResourcesMapper().get_resources()
            self._mqtt_publisher.publish_message(self.get_kpi_dict(payload, self._STR_DEVICE), retained)
            time.sleep(self._timer_test)
            self._mqtt_publisher.publish_message(self.get_kpi_dict(self._resources_dict, self._STR_DEVICE_TEST), retained)

    def get_kpi_dict(self, resources, device):
        kpi_dict = {}
        tmp = {}
        lista = []

        for resource in resources:
            tmp[resource] = resources[resource].get_value()

        tmp["cadence_production_line"] = tmp["daily_done_bags"] / (self._timer + self._timer_test)
        if tmp["sum_inference_time"] != 0:
            tmp["cobot_inactivity_factor"] = tmp["sum_cycle_time"] / tmp["sum_inference_time"]
        else:
            tmp["cobot_inactivity_factor"] = 0
        tmp["plant_inactivity_factor"] = tmp["sum_cycle_time"] / (self._timer + self._timer_test)

        lista.append(tmp)
        kpi_dict[device] = lista
        return json.dumps(kpi_dict)

    def stop(self):
        self._mqtt_client.loop_stop()
        self._mqtt_client.disconnect()
