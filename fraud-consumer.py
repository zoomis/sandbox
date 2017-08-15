import pulsar
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
logging.info('Connecting to Pulsar...')
client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('persistent://sample/standalone/ns1/fraudulent', 'my-sub')

logging.info('Created consumer')