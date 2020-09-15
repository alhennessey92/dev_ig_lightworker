
import time
import sys
import traceback
import logging


from protobuf import lightbringer_pb2
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
import json
import ccloud_lib
from uuid import uuid4

epic = "CHART:CS.D.EURUSD.MINI.IP:1MINUTE"

if __name__ == "__main__":
    # Read arguments and configurations and initialize
    config_file = 'librdkafka.config'
    topic = "eurusd"
    conf = ccloud_lib.read_ccloud_config(config_file)

    schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['schema.registry.basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_deserializer = ProtobufDeserializer(lightbringer_pb2.CandlePrice)
    string_deserializer = StringDeserializer('utf_8')

    # for full list of configurations, see:
    #   https://docs.confluent.io/current/clients/confluent-kafka-python/#deserializingconsumer
    consumer_conf = {
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'key.deserializer': string_deserializer,
        'value.deserializer': value_deserializer,
        'group.id': 'consumer_test',
        'auto.offset.reset': 'earliest' }

    consumer = DeserializingConsumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            candle = msg.value()
            if candle is not None:
                print("Candle record {}: Offer High: {}\n"
                      "\tOffer Low: {}\n"
                      "\tOffer Close: {}\n"
                      .format(msg.key(), candle.ofr_high,
                              candle.ofr_low,
                              candle.ofr_close))
        except KeyboardInterrupt:
            break

    consumer.close()






# What we need to do
# 1. Faust
# 
# 
# 
# 
# 