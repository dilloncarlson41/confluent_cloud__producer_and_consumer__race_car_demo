import os
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class Driver_in_loop(object):
    def __init__(self, race_car_id, tire_pressure, speed_mph):
        self.race_car_id = race_car_id
        self.tire_pressure = tire_pressure
        self.speed_mph = speed_mph


def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("../client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def read_sr_config():
  config = {}
  with open("../schema_registry.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


configs=read_config()
client_configs_dict=read_sr_config()


def dict_to_user(obj, ctx):
    if obj is None:
        return None
    return Driver_in_loop(race_car_id=obj['race_car_id'],
                tire_pressure=obj['tire_pressure'],
                speed_mph=obj['speed_mph'])


def main():
    topic = "driver_in_loop_telemetry"
    schema = "driver_in_loop_telemetry.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/{schema}") as f:
        schema_str = f.read()
    
    schema_registry_conf = {'url': f"""{client_configs_dict['sr.url']}""",'basic.auth.user.info':f"""{client_configs_dict['sr.key']}:{client_configs_dict['sr.secret']}"""}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                     schema_str,
                                     dict_to_user)

    consumer_conf = {'bootstrap.servers': f"""{configs['bootstrap.servers']}""",
                     'security.protocol':f"""{configs['security.protocol']}""",
                     'sasl.mechanisms':f"""{configs['sasl.mechanisms']}""",
                     'sasl.username':f"""{configs['sasl.username']}""",
                     'sasl.password':f"""{configs['sasl.password']}""",
                     'group.id': "driver_in_loop_consumer_1",
                     'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            driver_in_loop = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if driver_in_loop is not None:
                print("User record {}: race_car_id: {}\n"
                      "\ttire_pressure: {}\n"
                      "\tspeed_mph: {}\n"
                      .format(msg.key(), driver_in_loop.race_car_id,
                              driver_in_loop.tire_pressure,
                              driver_in_loop.speed_mph))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    main()