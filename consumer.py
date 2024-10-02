
#!c:/Users/langs/git/kafka/env/Scripts/python.exe
from confluent_kafka import Consumer, OFFSET_BEGINNING
from argparse import ArgumentParser
import time, json

SHOULD_RUN = True

consumer = Consumer(
    {
        'bootstrap.servers': '87.104.251.99',
        'group.id': 'POST',
        'auto.offset.reset': 'earliest'
    }
)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    consumer.subscribe(["POST"], on_assign=reset_offset)

    try:
        while SHOULD_RUN:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    print("Consumer Waiting...")
                    continue
                elif msg.error():
                    print(msg.error())
                    print("ERROR: %s".format(msg.error()))
                else:
                    value = json.loads(msg.value())
                    print(value)
            except:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()