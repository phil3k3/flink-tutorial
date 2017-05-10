package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class TaxiRideExercise {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "myGroup");

        DataStream<TaxiRide> rides = environment.addSource(new TaxiRideSource("/home/lip/projects/flinkquickstart/nycTaxiRides.gz", 60));

        DataStream<TaxiRide> ridesInNYC = rides.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                        GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
        });

        ridesInNYC.print();

        environment.execute("Taxi Rides in NYC");
    }
}
