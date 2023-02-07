import logging
import uuid
import paho.mqtt.client as mqtt
import yaml
from error.configuration_file_error import ConfigurationFileError
from error.mqtt.mqtt_client_connection_error import MqttClientConnectionError


class MqttTelemetryCollector:

    _STR_CLIENT_ID = "client_id"
    _STR_MODBUS_DEVICE_TYPE = "modbus_device_type"
    _STR_MODBUS_ADDRESS = "modbus_address"
    _STR_QE_POWER_T = "qe_power_t"
    _STR_BROKER_ADDRESS = "broker_address"
    _STR_BROKER_PORT = "broker_port"
    _STR_MQTT_USERNAME = "mqtt_username"
    _STR_MQTT_PASSWORD = "mqtt_password"
    _STR_QOS_PUBLISH = "qos_publish"
    _STR_QOS_SUBSCRIBE = "qos_subscribe"
    _STR_BASE_TOPIC = "base_topic"
    _STR_TELEMETRY_TOPIC = "telemetry_topic"
    _STR_ACTION_TOPIC = "action_topic"

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
                with open('../../config/MQTT/mqtt_telemetry_collector_config.yaml', 'r') as file:
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

        if self._STR_ACTION_TOPIC in self._mapper and self._mapper[self._STR_ACTION_TOPIC] is not None:
            self._action_topic = self._mapper[self._STR_ACTION_TOPIC]
        else:
            self._action_topic = "/action/result"

        self._mqtt_client = None
        logging.basicConfig(level=logging.INFO)

    def on_connect(self, client, userdata, flags, rc):
        logging.info("Client " + str(self._client_id) + " connected with result code " + str(rc))

    def on_disconnect(self, client, userdata, rc):
        logging.info("Client " + str(self._client_id) + " disconnected with result code " + str(rc))

    def on_message(self, client, userdata, message):
        string_payload = str(message.payload.decode("utf-8"))
        topic = message.topic
        logging.info(f"Received IoT Message: Topic: {topic} Payload: {string_payload}")

    def init(self):
        try:
            self._mqtt_client = mqtt.Client(self._client_id, clean_session=False)
            self._mqtt_client.on_connect = self.on_connect
            self._mqtt_client.on_message = self.on_message
            self._mqtt_client.on_disconnect = self.on_disconnect
            self._mqtt_client.username_pw_set(self._mqtt_username, self._mqtt_password)
            logging.info("Connecting to " + self._broker_address + " port: " + str(self._broker_port))
            self._mqtt_client.connect(self._broker_address, self._broker_port)
            self._mqtt_client.loop_start()
        except Exception as e:
            logging.error(str(e))
            raise MqttClientConnectionError("Error during mqtt client connection") from None

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

    def publish_senml_pack(self, topic, pack, retained=False):
        payload = pack.senml_pack_toString()
        self._mqtt_client.publish(topic, payload, qos=self._qos_publish, retain=retained)
        logging.info(f"Publish to topic: {topic} message: {payload}")

    def subscribe_topic(self, topic):
        self._mqtt_client.subscribe(topic, qos=self._qos_subscribe)
        logging.info(f"Subscribe to topic: {topic}")

    def stop(self):
        self._mqtt_client.loop_stop()
        self._mqtt_client.disconnect()
