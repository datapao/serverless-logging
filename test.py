import sys
import random
from random import gauss
import uuid
from logger import KinesisLogger

COLORS = ["blue","magenta", "red", "yellow", "green", "cyan"]

def create_message():
    message = {
        "color": random.choice(COLORS),
        "id": str(uuid.uuid4()),
        "value": gauss(35, 30),
        "source": "value"
    }
    return message


def main(stream, messages_num):
    logger = KinesisLogger(stream)
    while messages_num > 0:
        current_batch_num = min(messages_num, 500)
        messages = [create_message() for current_batch_num in range(500)]
        logger.log_batch(messages)
        messages_num -= current_batch_num


if __name__ == '__main__':
    stream_param = sys.argv[1]
    messages_num_param = int(sys.argv[2])
    main(stream_param, messages_num_param)
