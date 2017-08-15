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
    private static String SUBSCRIPTION = "subscriber-1";

    public FraudDetectionTopology() {}

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TupleToMessageMapper fraudulentNumberMapper = new TupleToMessageMapper() {
            @Override
            public Message toMessage(Tuple tuple) {
                long ccNumber = tuple.getLong(0);
                String msgStr = String.format(
                        "{\"fraudulent\":%d}", ccNumber
                );
                return MessageBuilder.create().setContent(
                        msgStr.getBytes())
                        .build();
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("fraudulent"));
            }
        };

        MessageToValuesMapper creditCardNumberMapper = new MessageToValuesMapper() {
            @Override
            public Values toValues(Message msg) {
                long num = Long.parseLong(new String(msg.getData()));
                return new Values(num);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("number"));
            }
        };

        PulsarSpout ccNumberSpout = new PulsarSpout.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(INPUT_TOPIC)
                .setServiceUrl(SUBSCRIPTION)
                .setMessageToValuesMapper(creditCardNumberMapper)
                .build();

        PulsarBolt fraudBolt = new PulsarBolt.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(OUTPUT_TOPIC)
                .setTupleToMessageMapper(fraudulentNumberMapper)
                .build();

        builder.setSpout("numbers", ccNumberSpout, 1);
        builder.setBolt("detect", new FraudDetectionBolt(), 1);

        Config conf = new Config();

        com.twitter.heron.api.Config.setComponentRam(conf,"numbers", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setComponentRam(conf,"detect", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 3);

        HelperRunner.runTopology(args, builder.createTopology(), conf);
    }
}
