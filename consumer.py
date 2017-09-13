import pulsar
import logging
import sys

TOPOLOGIES = ['word-count', 'pattern-detection']
WORD_COUNT_TOPIC = 'persistent://sample/standalone/ns1/wordcount'
PATTERN_DETECTION_TOPIC = 'persistent://sample/standalone/ns1/detected-patterns'
SUBSCRIPTION = 'pattern-detection-subscription-1'
TIMEOUT = 10000

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)


def run_consumer(client, topic):
    consumer = client.subscribe(topic, SUBSCRIPTION)
    logging.info('Created consumer for the topic %s', topic)
    while True:
        try:
            # try and receive messages with a timeout of 10 seconds
            msg = consumer.receive(timeout_millis=TIMEOUT)
            logging.info("Received message '%s'", msg.data())
            consumer.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
            logging.info("No message received in the last 10 seconds")


def main(args):
    if len(args) < 2:
        topology = 'word-count'
    else:
        topology = args[1]

    if not topology in TOPOLOGIES:
        logging.fatal('The topology %s is not amongst the available topologies: %s', topology, ", ".join(TOPOLOGIES))
        sys.exit(1)

    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    if topology == 'word-count':
        logging.info("Running the word count producer...")
        run_consumer(client, WORD_COUNT_TOPIC)

    elif topology == 'pattern-detection':
        logging.info("Running the pattern detection producer...")
        run_consumer(client, PATTERN_DETECTION_TOPIC)

    client.close()

if __name__ == '__main__':
    main(sys.argv)