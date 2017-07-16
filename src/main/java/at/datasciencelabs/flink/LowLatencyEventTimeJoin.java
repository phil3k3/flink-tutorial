package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import com.dataartisans.flinktraining.exercises.datastream_java.process.EventTimeJoinFunction;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.FinSources;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The Low Latency Event Time Join exercise of the Apache Flink training.
 * {@see http://training.data-artisans.com/exercises/eventTimeJoin.html
 */
public class LowLatencyEventTimeJoin {

    private static final String CUSTOMER_ID = "customerId";

    private static DataStream<Trade> tradeDataStream;
    private static DataStream<Customer> customerDataStream;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        setupDataStreams(executionEnvironment);

        DataStream<EnrichedTrade> enrichedTradeDataStream = tradeDataStream
                .keyBy(CUSTOMER_ID)
                .connect(customerDataStream.keyBy(CUSTOMER_ID))
                .process(new EventTimeJoinFunction());

        enrichedTradeDataStream.print();
        executionEnvironment.execute("Low Latency Event Time Join");
    }

    private static void setupDataStreams(StreamExecutionEnvironment executionEnvironment) {
        tradeDataStream = FinSources.tradeSource(executionEnvironment);
        customerDataStream = FinSources.customerSource(executionEnvironment);
    }
}
