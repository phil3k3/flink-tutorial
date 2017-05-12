package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class TravelTimePrediction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // enable checkpoint barriers to be emitted every second
        executionEnvironment.enableCheckpointing(1000);

        // try to restart job 6 times with 10 seconds delay in between attempts
        executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.of(10, TimeUnit.SECONDS)));

        DataStream<TaxiRide> dataStream = executionEnvironment.addSource(new CheckpointedTaxiRideSource("/home/phil3k/projects/flink-tutorial/nycTaxiRides.gz", 60));

        dataStream
                .filter(new FilterFunction<TaxiRide>() {
                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                                GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
                    }
                })
                .map(new MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>>() {
                    @Override
                    public Tuple2<Integer, TaxiRide> map(TaxiRide taxiRide) throws Exception {
                        return new Tuple2<>(GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat), taxiRide);
                    }
                })
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<Long, Integer>>() {

                    private transient ValueState<TravelTimePredictionModel> state;

                    @Override
                    public void flatMap(Tuple2<Integer, TaxiRide> taxiRide, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        TaxiRide taxiRide1 = taxiRide.f1;
                        TravelTimePredictionModel model = state.value();
                        if (model == null) {
                            model = new TravelTimePredictionModel();
                        }

                        int direction = GeoUtils.getDirectionAngle(taxiRide1.startLon, taxiRide1.startLat, taxiRide1.endLon, taxiRide1.endLat);
                        double distance = GeoUtils.getEuclideanDistance(taxiRide1.startLon, taxiRide1.startLat, taxiRide1.endLon, taxiRide1.endLat);
                        if (taxiRide1.isStart) {
                            int travelTime = model.predictTravelTime(direction, distance);
                            collector.collect(new Tuple2<>(taxiRide1.rideId, travelTime));
                        } else {
                            double travelTime = (taxiRide1.endTime.getMillis() - taxiRide1.startTime.getMillis()) / 60000.0;
                            model.refineModel(direction, distance, travelTime);
                            state.update(model);
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<TravelTimePredictionModel> descriptor =
                                new ValueStateDescriptor<>(
                                        "regressionModel",
                                        TravelTimePredictionModel.class);
                        state = getRuntimeContext().getState(descriptor);
                    }
                })
                .print();

        executionEnvironment.execute("Travel Time Prediction");
    }
}
