import pulsar
import random
from time import sleep
import logging

# This iterates continuously through a list sequence in random order
def random_cycle(ls):
    local_ls = ls[:]  # create defensive copy
    while True:
        random.shuffle(local_ls)
        for e in local_ls:
            yield e

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

logging.info('Connecting to Pulsar...')

# Create a pulsar client instance with reference to the broker
client = pulsar.Client('pulsar://localhost:6650')

# Build a producer instance on a specific topic
producer = client.create_producer('persistent://sample/standalone/ns1/sentences')

logging.info('Connected to Pulsar')

# Collection of sentences to serve as random input sequence
sentences = random_cycle([
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        ])

logging.info('Sending Messages ...')

for sentence in sentences:
    sleep(0.05) # throttle messages with 50 ms delay
    logging.info('Sending message - %s ', sentence)
    producer.send(sentence) # publish to pulsar

client.close()
