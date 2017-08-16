import pulsar
import random
from random import randint
from time import sleep
import logging
import sys

TOPOLOGIES = ['word-count', 'fraud-detection']
WORD_COUNT_TOPIC = 'persistent://sample/standalone/ns1/sentences'
FRAUD_DETECTION_TOPIC = 'persistent://sample/standalone/ns1/credit-card-numbers'

# Setup for basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)


# This iterates continuously through a list sequence in random order
def random_cycle(ls):
    local_ls = ls[:]  # create defensive copy
    while True:
        random.shuffle(local_ls)
        for e in local_ls:
            yield e


def run_word_count_producer(client):
    # Build a producer instance on a specific topic
    producer = client.create_producer(WORD_COUNT_TOPIC)
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



def run_fraud_detection_producer(client):
    producer = client.create_producer(FRAUD_DETECTION_TOPIC)

    def random_cc_number():
        return ''.join(["%s" % randint(0, 9) for num in range(0, 16)])

    logging.info('Sending random credit card numbers to fraud detection topology...')

    while True:
        sleep(0.05)
        num = random_cc_number()
        logging.info('Sending credit card number: %s', num)
        producer.send(num)


def main(args):
    if len(args) < 2:
        logging.fatal("You must supply a topology to target")
        sys.exit(1)

    topology = args[1]

    if not topology in TOPOLOGIES:
        logging.fatal('The topology %s is not amongst the available topologies: %s', topology, ", ".join(TOPOLOGIES))
        sys.exit(1)

    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    if topology == 'word-count':
        logging.info("Running the word count producer...")
        run_word_count_producer(client)

    elif topology == 'fraud-detection':
        logging.info("Running the fraud detection producer...")
        run_fraud_detection_producer(client)

    client.close()


if __name__ == '__main__':
    main(sys.argv)