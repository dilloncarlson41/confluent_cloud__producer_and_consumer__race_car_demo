from confluent_kafka import Producer
from random import choice, randint
import time
import os
from uuid import uuid4
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class RaceCarDetail(object):
    def __init__(self, car_id, number, color, make):
        self.car_id = car_id
        self.number = number
        self.color = color
        self.make = make


def read_config():
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


client_configs_dict = read_sr_config()


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def _race_car_to_dict(race_car_detail, ctx):
    return dict(car_id=race_car_detail.car_id,
                number=race_car_detail.number,
                color=race_car_detail.color,
                make=race_car_detail.make)


def main():
    topic = "race_car_detail"
    key_schema = "race_car_detail__key.avsc"
    value_schema = "race_car_detail__value.avsc"
    path = os.path.realpath(os.path.dirname(__file__))

    # Load schemas
    with open(f"{path}/{key_schema}") as f:
        key_schema_str = f.read()
    with open(f"{path}/{value_schema}") as f:
        value_schema_str = f.read()

    # Schema Registry configuration
    schema_registry_conf = {
        'url': f"""{client_configs_dict['sr.url']}""",
        'basic.auth.user.info': f"""{client_configs_dict['sr.key']}:{client_configs_dict['sr.secret']}"""
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Avro serializers
    key_avro_serializer = AvroSerializer(schema_registry_client, key_schema_str, lambda x, ctx: {"car_id": x})
    value_avro_serializer = AvroSerializer(schema_registry_client, value_schema_str, _race_car_to_dict)
    
    # Producer configuration
    configs = read_config()
    producer = Producer(configs)

    print(f"Producing records to topic {topic}. Press Ctrl+C to exit.")
    records_to_produce = 50
    records_produced = 0

    car_colors = ["Red", "Blue", "Green", "Black", "White"]
    car_makes = ["Ford", "Chevrolet", "Tesla", "Toyota"]

    try:
        while records_produced < records_to_produce:
            producer.poll(0.0)

            # Randomize car details
            car_id = str(uuid4())
            number = randint(0, 99)
            color = choice(car_colors)
            make = choice(car_makes)

            race_car_detail = RaceCarDetail(car_id=car_id, number=number, color=color, make=make)

            # Produce message
            producer.produce(
                topic=topic,
                key=key_avro_serializer(car_id, SerializationContext(topic, MessageField.KEY)),
                value=value_avro_serializer(race_car_detail, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=delivery_report
            )

            time.sleep(1)
            records_produced += 1
            print(f"Produced {records_produced}/{records_to_produce} records.")

    except KeyboardInterrupt:
        print("Production interrupted.")
    finally:
        producer.flush()
        print("Flushed remaining records.")


if __name__ == '__main__':
    main()