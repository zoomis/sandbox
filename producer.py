import pulsar
import random
from time import sleep
import logging

def random_cycle(ls):
        local_ls = ls[:] # copy
        while True:
            random.shuffle(local_ls)
            for e in local_ls:
                yield e

logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

logging.info('Connecting to Pulsar...')

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('persistent://sample/standalone/ns/my-topic')

logging.info('Connected to Pulsar')

sentences = random_cycle([
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        ])

logging.info('Sending Messages ...')

for sentence in sentences:
	sleep(0.05)
	logging.info('Sending message - %s ', sentence)
	producer.send(sentence)

client.close()