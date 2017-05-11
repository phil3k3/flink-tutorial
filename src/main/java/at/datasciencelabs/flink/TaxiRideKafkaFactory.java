package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class TaxiRideKafkaFactory {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String ZOOKEEPER = "localhost:2181";
    private static final String KAFKA_TOPIC = "rides";

    static FlinkKafkaProducer010<TaxiRide> createKafkaProducer() {
        return new FlinkKafkaProducer010<>(KAFKA_BROKER, KAFKA_TOPIC, new TaxiRideSchema());
    }

    static FlinkKafkaConsumer010<TaxiRide> createKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", ZOOKEEPER);
        properties.setProperty("bootstrap.servers", KAFKA_BROKER);
        properties.setProperty("group.id", "myGroup");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer010<TaxiRide> ridesConsumer = new FlinkKafkaConsumer010<>(KAFKA_TOPIC, new TaxiRideSchema(), properties);
        ridesConsumer.assignTimestampsAndWatermarks(new TaxiRideBoundedOutOfOrdernessTimestampExtractor());
        return ridesConsumer;
    }

    private static class TaxiRideBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

        TaxiRideBoundedOutOfOrdernessTimestampExtractor() {
            super(Time.of(5, TimeUnit.SECONDS));
        }

        @Override
        public long extractTimestamp(TaxiRide taxiRide) {
            if (taxiRide.isStart) {
                return taxiRide.startTime.getMillis();
            }
            return taxiRide.endTime.getMillis();
        }
    }
}
