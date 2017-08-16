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
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String ccNumber = tuple.getString(0);

        Set<String> patterns = new HashSet<String>() {{
           add("123");
           add("567");
        }};

        patterns.forEach(p -> {
            if (ccNumber.contains(p)) {
                collector.emit(new Values(ccNumber));
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fraud"));
    }
}