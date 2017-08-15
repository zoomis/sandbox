import pulsar
from time import sleep
import logging

def random_cc_number():
    return ''.join(["%s" % randint(0, 9) for num in range(0, 16)])

logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

logging.info('Connecting to Pulsar...')

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('persistent://sample/standalone/ns1/credit-card-numbers')

while True:
    sleep(0.05)
    num = random_cc_number()
    logging.info('Sending credit card number - %s', num)
    producer.send(num)

client.close()