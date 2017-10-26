package tutorial;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pulsar.PulsarBolt;

import java.text.BreakIterator;

/**
 * This Bolt splits a sentence into words
 */
public class SplitSentenceBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarBolt.class);

    //Execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //Get the sentence content from the tuple
        String sentence = tuple.getString(0);

        //An iterator to get each word
        BreakIterator boundary = BreakIterator.getWordInstance();
        //Give the iterator the sentence
        boundary.setText(sentence);
        //Find the beginning first word
        int start = boundary.first();
        //Iterate over each word and emit it to the output stream
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            //get the word
            String word = sentence.substring(start, end);
            //If a word is whitespace characters, replace it with empty
            word = word.replaceAll("\\s+", "");
            //if it's an actual word, emit it
            if (!word.equals("")) {
                collector.emit(new Values(word));
            }
        }
    }

    //Declare that emitted tuples contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}