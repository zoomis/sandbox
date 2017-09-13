package tutorial;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class PatternDetectionBolt extends BaseBasicBolt {
    private Set<String> patterns;

    public PatternDetectionBolt() {
        patterns = new HashSet<>();
    }

    private void detectPatterns(String number, BasicOutputCollector collector) {
        patterns.forEach(p -> {
            if (number.contains(p)) {
                collector.emit(new Values(p, number));
            }
        });
    }

    private void addPatternsToList(Tuple tuple) {
        String[] newPatterns = tuple.getString(tuple.fieldIndex("pattern")).split(",");
        Collections.addAll(patterns, newPatterns);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals("incoming-numbers")) {
            String ccNumber = tuple.getString(tuple.fieldIndex("number"));
            detectPatterns(ccNumber, collector);
        }

        if (tuple.getSourceComponent().equals("add-pattern")) {
           addPatternsToList(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("pattern", "original-number"));
    }
}