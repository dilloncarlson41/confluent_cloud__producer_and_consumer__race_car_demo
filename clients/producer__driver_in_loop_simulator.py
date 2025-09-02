from confluent_kafka import Producer
from random import randint
import time
import os
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


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


client_configs_dict=read_sr_config()


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def _telemetry_to_dict(driver_in_loop,ctx):
  return dict(race_car_id=driver_in_loop.race_car_id,
                tire_pressure=driver_in_loop.tire_pressure,
                speed_mph=driver_in_loop.speed_mph)


def main():
    topic = "driver_in_loop_telemetry"
    schema = "driver_in_loop_telemetry.avsc"
    path = os.path.realpath(os.path.dirname(__file__))


    with open(f"{path}/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': f"""{client_configs_dict['sr.url']}""",'basic.auth.user.info':f"""{client_configs_dict['sr.key']}:{client_configs_dict['sr.secret']}"""}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     _telemetry_to_dict)

    string_serializer = StringSerializer('utf_8')
    configs=read_config()
    producer = Producer(configs)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    records_to_produce=50
    records_produced=0
    while records_produced<records_to_produce:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            race_car_id='test1'
            tire_pressure=randint(20, 60)
            speed_mph=randint(0,250)
            driver_in_loop = Driver_in_loop(race_car_id=race_car_id,
                        tire_pressure=tire_pressure,
                        speed_mph=speed_mph)
            producer.produce(topic=topic,
                             key=string_serializer(race_car_id),
                             value=avro_serializer(driver_in_loop, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue
        time.sleep(1)
        records_produced+=1
        producer.flush()
        print(f"\nFlushing {records_produced} records...")


if __name__ == '__main__':
    main()