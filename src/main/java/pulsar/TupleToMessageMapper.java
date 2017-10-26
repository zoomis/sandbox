package pulsar;

import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Tuple;
import org.apache.pulsar.client.api.Message;

import java.io.Serializable;

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
