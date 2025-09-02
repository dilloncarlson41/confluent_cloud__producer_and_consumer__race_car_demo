# Kafka Producer and Consumer with Confluent Cloud (Python 3.8+)

This repository provides a simple example of a **Kafka Producer** and **Consumer** using **Python 3.8+** that works with [Confluent Cloud](https://www.confluent.io/confluent-cloud/). It demonstrates how to send and receive Avro-encoded messages using the Confluent Schema Registry.

---

## Features

- Produces randomly generated messages for race car telemetry data (speed and tire pressure)
- Serializes data using **Avro** with Confluent Schema Registry
- Sends messages to and consumes from a topic in **Confluent Cloud**
- Create alerts based on thresholds for race cars (tire pressure too low, speed is high)
- Clean, simple Python implementation for learning and prototyping

---

## Requirements

- Python **3.8+**
- A [Confluent Cloud account](https://www.confluent.io/confluent-cloud/)
- A Kafka topic created in Confluent Cloud
- API credentials for Kafka and Schema Registry

---

## 1. Setup

### Install Python Dependencies in a Terminal

```pip install confluent-kafka confluent-kafka[avro]```


## 2. Run the python producer
- Get a kafka api key and secret from Confluent Cloud and replace the username and password in the client.properties file
- Get a schema registry api key and secret from Confluent Cloud and replace the username and password in the schema-registry.properties file
- Run the producer script in a terminal
```cd race_cars/clients```
```python producer__race_car_detail.py```


## 3. Running Flink SQL Statements in Confluent Cloud UI
- Log in to Confluent Cloud.
- Navigate to your Kafka cluster.
- Go to the "Flink SQL" section in the left sidebar.
- Create a new Flink SQL statement:
- Insert Flink statements from the flink.sql file and click Run


## 4. Run the python consumer
- Run the consumer script in a terminal
```cd race_cars/clients```
```python consumer__race_car_detail.py```


## Notes
Run the producer after the Flink statements have executed and the consumer is running to see a full flow of messages in near real time. 
