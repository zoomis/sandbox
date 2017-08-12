
import pulsar
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

logging.info('Connecting to Pulsar...')

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('persistent://sample/standalone/ns1/wordcount', 'my-sub')

logging.info('Created consumer')

while True:
    try:
        msg = consumer.receive(timeout_millis=5000)
        logging.info("Received message '%s' id='%s'", msg.data(), msg.message_id())
        consumer.acknowledge(msg)
    except Exception:
        logging.info("No message received, Closing connection")
        client.close()
