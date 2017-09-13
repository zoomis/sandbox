package tutorial;

import com.twitter.heron.common.basics.ByteAmount;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pulsar.MessageToValuesMapper;
import pulsar.PulsarBolt;
import pulsar.PulsarSpout;
import pulsar.TupleToMessageMapper;
import tutorial.util.Constants;
import tutorial.util.HelperRunner;

public class PatternDetectionTopology {
    private static String INPUT_TOPIC = "persistent://sample/standalone/ns1/random-numbers";
    private static String OUTPUT_TOPIC = "persistent://sample/standalone/ns1/detected-patterns";
    private static String SUBSCRIPTION = "pattern-detection-subscription";
    private static String FRAUD_NUMBER_TOPIC = "persistent://sample/standalone/ns1/add-pattern";

    public PatternDetectionTopology() {}

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TupleToMessageMapper detectedPatternMapper = new TupleToMessageMapper() {
            @Override
            public Message toMessage(Tuple tuple) {
                String pattern = tuple.getString(tuple.fieldIndex("pattern"));
                String number = tuple.getString(tuple.fieldIndex("original-number"));

                String msg = String.format("Detected pattern %s in number %s", pattern, number);

                byte[] msgPayload = msg.getBytes();

                return MessageBuilder.create().setContent(msgPayload)
                        .build();
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("detected-pattern"));
            }
        };

        MessageToValuesMapper incomingNumberMapper = new MessageToValuesMapper() {
            @Override
            public Values toValues(Message msg) {
                return new Values(new String(msg.getData()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("number"));
            }
        };

        MessageToValuesMapper addPatternMapper = new MessageToValuesMapper() {
            @Override
            public Values toValues(Message msg) {
                return new Values(new String(msg.getData()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("pattern"));
            }
        };

        PulsarSpout incomingNumberSpout = new PulsarSpout.Builder()
                .setServiceUrl(Constants.PULSAR_SERVICE_URL)
                .setTopic(INPUT_TOPIC)
                .setSubscription(SUBSCRIPTION)
                .setMessageToValuesMapper(incomingNumberMapper)
                .build();

        PulsarSpout addPatternSpout = new PulsarSpout.Builder()
                .setServiceUrl(Constants.PULSAR_SERVICE_URL)
                .setTopic(FRAUD_NUMBER_TOPIC)
                .setSubscription(SUBSCRIPTION)
                .setMessageToValuesMapper(addPatternMapper)
                .build();

        PulsarBolt detectedPatternOutputBolt = new PulsarBolt.Builder()
                .setServiceUrl(Constants.PULSAR_SERVICE_URL)
                .setTopic(OUTPUT_TOPIC)
                .setTupleToMessageMapper(detectedPatternMapper)
                .build();

        builder.setSpout("incoming-numbers", incomingNumberSpout, 1);
        builder.setSpout("add-pattern", addPatternSpout, 1);

        builder.setBolt("pattern-detection", new PatternDetectionBolt(), 2)
                .fieldsGrouping("incoming-numbers", new Fields("number"))
                .fieldsGrouping("add-pattern", new Fields("pattern"));

        builder.setBolt("pulsar-output", detectedPatternOutputBolt, 1)
                .globalGrouping("pattern-detection");

        Config conf = new Config();

        conf.setNumWorkers(2);

        com.twitter.heron.api.Config.setComponentRam(conf,"incoming-numbers", ByteAmount.fromMegabytes(256));
        com.twitter.heron.api.Config.setComponentRam(conf,"pattern-detection", ByteAmount.fromMegabytes(256));
        com.twitter.heron.api.Config.setComponentRam(conf,"pulsar-output", ByteAmount.fromMegabytes(256));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 0.5f);

        HelperRunner.runTopology(args, builder.createTopology(), conf);
    }
}
