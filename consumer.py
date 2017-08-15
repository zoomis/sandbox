import pulsar
import logging

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

logging.info('Connecting to Pulsar...')

# Create a pulsar client instance with reference to the broker
client = pulsar.Client('pulsar://localhost:6650')

# Build a producer instance on a topic queue with an id
consumer = client.subscribe('persistent://sample/standalone/ns1/wordcount', 'my-sub')

logging.info('Created consumer')

while True:
    try:
        # try and receive messages with a timeout of 10 seconds
        msg = consumer.receive(timeout_millis=10000)
        logging.info("Received message '%s'", msg.data())
        consumer.acknowledge(msg)  # send ack to pulsar for message consumption
    except Exception:
        logging.info("No message received in the last 10 seconds")

client.close()
