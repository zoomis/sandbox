package tutorial;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;

public class FraudDetectionBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String ccNumber = tuple.getString(0);
        int result = ccNumber.compareTo("5000000000000000");

        if (result > 0) { // Number is greater than 500000000000
            collector.emit(new Values(ccNumber));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fraudulent"));
    }
}