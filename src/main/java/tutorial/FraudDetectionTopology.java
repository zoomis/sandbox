package tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import tutorial.util.HelperRunner;

import java.util.HashMap;
import java.util.Map;

public class FraudDetectionTopology {
    private static String SERVICE_URL = "pulsar://localhost:6650";
    private static String INPUT_TOPIC = "persistent://sample/standalone/ns1/credit-card-numbers";
    private static String OUTPUT_TOPIC = "persistent://sample/standalone/ns1/fraud";
    private static String SUBSCRIPTION = "cc-number-subscription";

    public FraudDetectionTopology() {}

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TupleToMessageMapper fraudulentNumberMapper = new TupleToMessageMapper() {
            @Override
            public Message toMessage(Tuple tuple) {
                String msg = String.format("Fraudulent number: %s", tuple.getString(0));

                return MessageBuilder.create().setContent(
                        msg.getBytes())
                        .build();
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("fraud"));
            }
        };

        MessageToValuesMapper creditCardNumberMapper = new MessageToValuesMapper() {
            @Override
            public Values toValues(Message msg) {
                return new Values(new String(msg.getData()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("number"));
            }
        };

        PulsarSpout ccNumberSpout = new PulsarSpout.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(INPUT_TOPIC)
                .setSubscription(SUBSCRIPTION)
                .setMessageToValuesMapper(creditCardNumberMapper)
                .build();

        PulsarBolt fraudBolt = new PulsarBolt.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(OUTPUT_TOPIC)
                .setTupleToMessageMapper(fraudulentNumberMapper)
                .build();

        builder.setSpout("numbers", ccNumberSpout, 1);
        builder.setBolt("fraud", new FraudDetectionBolt(), 1).fieldsGrouping("numbers", new Fields("number"));
        builder.setBolt("pulsar", fraudBolt, 1).globalGrouping("fraud");

        Config conf = new Config();

        conf.setNumWorkers(2);

        com.twitter.heron.api.Config.setComponentRam(conf,"numbers", ByteAmount.fromMegabytes(256));
        //com.twitter.heron.api.Config.setComponentRam(conf,"fraud", ByteAmount.fromMegabytes(256));
        com.twitter.heron.api.Config.setComponentRam(conf,"pulsar", ByteAmount.fromMegabytes(256));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 0.5f);

        HelperRunner.runTopology(args, builder.createTopology(), conf);
    }
}
