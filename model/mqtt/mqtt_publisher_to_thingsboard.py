import logging
import uuid
import yaml
import paho.mqtt.client as mqtt
from error.configuration_file_error import ConfigurationFileError
from error.mqtt.mqtt_client_connection_error import MqttClientConnectionError


class MqttPublisherToThingsboard:
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
                with open('../../../config/mqtt/mqtt_telemetry_publisher.yaml', 'r') as file:
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

        self._mqtt_client = None

    def init(self):
        try:
            self._mqtt_client = mqtt.Client(self._client_id, clean_session=False)
            self._mqtt_client.username_pw_set(self._mqtt_username, self._mqtt_password)
            print("Connecting to " + self._broker_address + " port: " + str(self._broker_port))
            self._mqtt_client.connect(self._broker_address, self._broker_port)
            self._mqtt_client.loop_start()
        except Exception as e:
            logging.error(str(e))
            raise MqttClientConnectionError("Error during mqtt client connection") from None

    def publish_message(self, payload, retained=False):
        topic = self._mqtt_basic_topic + self._mqtt_telemetry_topic
        self._mqtt_client.publish(topic, payload, qos=self._qos_publish, retain=retained)
        print(f"Publish to topic: {topic} message: {payload}")
