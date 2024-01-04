import pyshark
from kafka import KafkaProducer
import kafka.errors
import time

KAFKA_TOPIC_NAME = "network-data"
KAFKA_BROKER = "kafka1:19092"


if __name__ == "__main__":


    print("Kafka producer app started ...")
    while True:
        try:
            kafka_producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER,
                                        value_serializer=lambda x: x.encode('utf-8'))
            print("Connected to Kafka!")
            break
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)

    cap = pyshark.LiveCapture("any")

    for packet in cap.sniff_continuously():
        try:
            src_ip = packet.ip.src
            dst_ip = packet.ip.dst

            print(f"Source IP: {src_ip} --> Destination IP: {dst_ip}")
            kafka_producer.send(KAFKA_TOPIC_NAME, bytes(f"Source IP: {src_ip} --> Destination IP: {dst_ip}", "utf-8"))
            kafka_producer.flush()

        except AttributeError as e:
            print(f"Skipping packet: {e}")

