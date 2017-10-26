package tutorial;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.PulsarClientException;
import tutorial.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

/**
 * This Bolt emits word count pairs in periodic intervals
 */
public class WordCountBolt extends BaseBasicBolt {
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(WordCountBolt.class);

    private static final int EMIT_FREQUENCY = 3;
    //For holding words and counts
    private Map<String, Integer> counts = new HashMap<>();
    //How often to emit a count of words
    private Integer emitFrequency;

    public WordCountBolt() throws PulsarClientException {
        emitFrequency = EMIT_FREQUENCY;
    }

    public WordCountBolt(int emitFrequency){
        this.emitFrequency = emitFrequency;
    }

    //Configure frequency of tick tuples for this bolt
    //This delivers a 'tick' tuple on a specific interval,
    //which is used to trigger certain actions
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS, emitFrequency);
        return conf;
    }

    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //If it's a tick tuple, emit all words and counts
        if(TupleHelpers.isTickTuple(tuple)) {
            for(String word : counts.keySet()) {
                Integer count = counts.get(word);
                collector.emit(new Values(word, count));
                logger.info(String.format("Emitting a count of (%d) for word (%s)", count, word));
            }
        } else {
            //Get the word contents from the tuple

            String word = tuple.getString(0);
            logger.info(word);
            //Have we counted any already?
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            //Increment the count and store it
            count++;
            counts.put(word, count);
        }

    }

    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}