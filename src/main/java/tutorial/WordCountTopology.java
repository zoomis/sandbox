package tutorial;

import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.ByteAmount;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import com.twitter.heron.api.Config;
import pulsar.MessageToValuesMapper;
import pulsar.PulsarBolt;
import pulsar.PulsarSpout;
import pulsar.TupleToMessageMapper;

/**
 * This is driver as well the topology graph generator
 */
public class WordCountTopology {

    private static String SERVICE_URL = "pulsar://localhost:6650";
    private static String INPUT_TOPIC = "persistent://sample/standalone/ns1/sentences";
    private static String OUTPUT_TOPIC = "persistent://sample/standalone/ns1/wordcount";
    private static String SUBSCRIPTION = "subscriber-1";

    private WordCountTopology() { }

    //Entry point for the topology
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        @SuppressWarnings("serial")
        TupleToMessageMapper wordCountMapper = new TupleToMessageMapper() {

            @Override
            public Message toMessage(Tuple tuple) {
                return MessageBuilder.create().setContent(
                        String.format(
                                "{ \"word\" : \"%s\" , \"count\" : %d }", tuple.getString(0), tuple.getInteger(1))
                                .getBytes())
                        //.setKey(tuple.getString(0))
                        .build();
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("word", "count"));
            }

        };


        @SuppressWarnings("serial")
        MessageToValuesMapper sentenceMapper = new MessageToValuesMapper() {

            @Override
            public Values toValues(Message msg) {
                return new Values(new String(msg.getData()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                // declare the output fields
                declarer.declare(new Fields("sentence"));
            }
        };

        PulsarSpout randomSentenceSpout = new PulsarSpout.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(INPUT_TOPIC)
                .setSubscription(SUBSCRIPTION)
                .setMessageToValuesMapper(sentenceMapper)
                .build();

        PulsarBolt messageBolt = new PulsarBolt.Builder()
                .setServiceUrl(SERVICE_URL)
                .setTopic(OUTPUT_TOPIC)
                .setTupleToMessageMapper(wordCountMapper)
                .build();

        builder.setSpout("sentence", randomSentenceSpout,1);
        builder.setBolt("split", new SplitSentenceBolt(),2).shuffleGrouping("sentence");
        builder.setBolt("count", new WordCountBolt(),2).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("pulsar", messageBolt,1).globalGrouping("count");

        Config conf = new Config();

        conf.setNumStmgrs(4);

        // Resource Configs
        conf.setComponentRam("sentence", ByteAmount.fromMegabytes(256));
        conf.setComponentRam( "split", ByteAmount.fromMegabytes(256));
        conf.setComponentRam( "count", ByteAmount.fromMegabytes(256));
        conf.setComponentRam( "pulsar", ByteAmount.fromMegabytes(256));
        conf.setContainerCpuRequested(0.5f);

        //submit the topology
        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

}
