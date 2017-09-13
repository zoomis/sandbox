package tutorial;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class FraudDetectionBolt extends BaseBasicBolt {
    private Set<String> fraudPatterns;

    public FraudDetectionBolt() {
        fraudPatterns = new HashSet<>();
    }

    private void detectPatterns(String ccNumber, BasicOutputCollector collector) {
        fraudPatterns.forEach(p -> {
            if (ccNumber.contains(p)) {
                collector.emit(new Values(ccNumber));
            }
        });
    }

    private void addPatternsToList(Tuple tuple) {
        String[] newPatterns = tuple.getString(tuple.fieldIndex("fraud-number")).split(",");
        Collections.addAll(fraudPatterns, newPatterns);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals("numbers")) {
            String ccNumber = tuple.getString(tuple.fieldIndex("number"));
            detectPatterns(ccNumber, collector);
        }

        if (tuple.getSourceComponent().equals("fraud-numbers")) {
           addPatternsToList(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fraud"));
    }
}