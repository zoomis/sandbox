import pulsar
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
logging.info('Connecting to Pulsar...')
client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('persistent://sample/standalone/ns1/fraud', 'sub-1')

logging.info('Created consumer')

while True:
    try:
        msg = consumer.receive(timeout_millis=10000)
        logging.info("Received message '%s", msg.data())
        consumer.acknowledge(msg)
    except Exception:
        logging.info("No message received in the last 10 seconds")

client.close()