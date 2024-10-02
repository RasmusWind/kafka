#!c:/Users/langs/git/kafka/env/Scripts/python.exe
from confluent_kafka import Producer
import time

producer = Producer(
    {'bootstrap.servers': '192.168.1.141'}
)

if __name__ == "__main__":
    for i in range(10):
        producer.produce("Test", str(i), str(int(time.time())))
        time.sleep(1)
    producer.flush()