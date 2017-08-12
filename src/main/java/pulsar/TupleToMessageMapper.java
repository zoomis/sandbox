package pulsar;

import java.io.Serializable;
import org.apache.pulsar.client.api.Message;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public interface TupleToMessageMapper extends Serializable {

    /**
     * Convert tuple to {@link org.apache.pulsar.client.api.Message}.
     *
     * @param tuple
     * @return
     */
    public Message toMessage(Tuple tuple);

    /**
     * Declare the output schema for the bolt.
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer);
}
