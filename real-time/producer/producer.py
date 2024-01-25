#!/usr/bin/env python

import pyshark
from kafka import KafkaProducer
import kafka.errors
import time
import sys
from json import dumps

KAFKA_TOPIC_NAME = "network-data"
KAFKA_BROKER = "localhost:9092"


if __name__ == "__main__":


    print("Kafka producer app started ...")
    sys.stdout.flush()

    while True:
    
        try:
            kafka_producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER,
                                        value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka!")
            sys.stdout.flush()
            break
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            sys.stdout.flush()
            time.sleep(3)
    
    cap = pyshark.LiveCapture("any")

    for packet in cap.sniff_continuously():
        try:
            src_ip = packet.ip.src
            dst_ip = packet.ip.dst
            transport_layer = packet.transport_layer
            src_port = packet[transport_layer].srcport
            dst_port = packet[transport_layer].dstport
            protocol = packet.transport_layer
            len = packet.length
            value_dict = {
                'src_ip': src_ip,
                'dst_ip': dst_ip,
                'src_port': src_port,
                'dst_port': dst_port,
                'protocol': protocol,
                'length': len
            }
            #print(f"Source IP: {src_ip} --> Destination IP: {dst_ip}")
            print(f"Source IP: {src_ip}, Source Port: {src_port}, Destination IP: {dst_ip}, Destination Port: {dst_port}, Protocol: {protocol}, Lwn {len}")
            sys.stdout.flush()
            #kafka_producer.send(KAFKA_TOPIC_NAME, value=f"Source IP: {src_ip} --> Destination IP: {dst_ip}")
            kafka_producer.send(KAFKA_TOPIC_NAME, value=value_dict)
            kafka_producer.flush()
            time.sleep(1)

        except AttributeError as e:
            print(f"Skipping packet: {e}")
            sys.stdout.flush()
    


