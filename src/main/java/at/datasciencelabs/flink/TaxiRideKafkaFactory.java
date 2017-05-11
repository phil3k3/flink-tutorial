package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

class TaxiRideKafkaFactory {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String ZOOKEEPER = "localhost:2181";
    private static final String KAFKA_TOPIC = "rides";

    static FlinkKafkaProducer010<TaxiRide> getKafkaProducer() {
        return new FlinkKafkaProducer010<>(KAFKA_BROKER, KAFKA_TOPIC, new TaxiRideSchema());
    }

    static FlinkKafkaConsumer010<TaxiRide> getKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", ZOOKEEPER);
        properties.setProperty("bootstrap.servers", KAFKA_BROKER);
        properties.setProperty("group.id", "myGroup");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer010<TaxiRide> ridesConsumer = new FlinkKafkaConsumer010<>(KAFKA_TOPIC, new TaxiRideSchema(), properties);
        ridesConsumer.assignTimestampsAndWatermarks(new TaxiRideBoundedOutOfOrdernessTimestampExtractor());
        return ridesConsumer;
    }
}
