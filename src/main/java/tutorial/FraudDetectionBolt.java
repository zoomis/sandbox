package tutorial;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;
import java.util.Set;

public class FraudDetectionBolt extends BaseBasicBolt {
    private Set<String> fraudPatterns;

    public FraudDetectionBolt() {
        fraudPatterns = new HashSet<>();
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals("numbers")) {
            String ccNumber = tuple.getString(tuple.fieldIndex("number"));

            fraudPatterns.forEach(p -> {
                if (ccNumber.contains(p)) {
                    collector.emit(new Values(ccNumber));
                }
            });
        }

        if (tuple.getSourceComponent().equals("fraud-numbers")) {
            String pattern = tuple.getString(tuple.fieldIndex("fraud-number"));
            fraudPatterns.add(pattern);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fraud"));
    }
}