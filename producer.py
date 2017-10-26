import pulsar
import random
from random import randint
from time import sleep
import logging
import sys

SENTENCES_TOPIC = 'persistent://sample/standalone/ns1/sentences'

# Setup for basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

# This iterates continuously through a list sequence in random order
def random_cycle(ls):
    local_ls = ls[:]  # create defensive copy
    while True:
        random.shuffle(local_ls)
        for e in local_ls:
            yield e

def main(args):
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    # Build a producer instance on a specific topic
    producer = client.create_producer(SENTENCES_TOPIC)
    logging.info('Connected to Pulsar')

     # Collection of sentences to serve as random input sequence
    sentences = random_cycle([
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
            ])

    logging.info('Sending sentences to the word count topology...')

    for sentence in sentences:
        sleep(0.05)  # Throttle messages with a 50 ms delay
        logging.info('Sending sentence: %s ', sentence)
        producer.send(sentence) # Publish randomly selected sentence to Pulsar

    client.close()


if __name__ == '__main__':
    main(sys.argv)