package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class PopularPlaces {

    private static int POPULARITY_THRESHOLD = 20;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create automatic watermarks every second
        executionEnvironment.getConfig().setAutoWatermarkInterval(1000);

        DataStream<TaxiRide> dataStream = executionEnvironment.addSource(TaxiRideKafkaFactory.getKafkaConsumer());

        SingleOutputStreamOperator<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlacesStream = dataStream.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                        GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
        }).map(new MapFunction<TaxiRide, Tuple2<Boolean, Integer>>() {
            @Override
            public Tuple2<Boolean, Integer> map(TaxiRide taxiRide) throws Exception {
                if (taxiRide.isStart) {
                    int startCellId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                    return Tuple2.of(true, startCellId);
                } else {
                    int endCellId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                    return Tuple2.of(false, endCellId);
                }
            }
        }).keyBy(0, 1)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new WindowFunction<Tuple2<Boolean, Integer>, Tuple4<Integer, Long, Boolean, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<Boolean, Integer>> timeWindowRecords, Collector<Tuple4<Integer, Long, Boolean, Integer>> collector) throws Exception {
                        Tuple2<Boolean, Integer> initial = ((Tuple2<Boolean, Integer>) key);
                        int count = 0;
                        for (Tuple2<Boolean, Integer> ignored : timeWindowRecords) {
                            count++;
                        }
                        collector.collect(new Tuple4<>(initial.f1, timeWindow.getEnd(), initial.f0, count));
                    }
                }).filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
            @Override
            public boolean filter(Tuple4<Integer, Long, Boolean, Integer> windowedRecordWithCount) throws Exception {
                return windowedRecordWithCount.f3 > POPULARITY_THRESHOLD;
            }
        }).map(new MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {
            @Override
            public Tuple5<Float, Float, Long, Boolean, Integer> map(Tuple4<Integer, Long, Boolean, Integer> filteredRide) throws Exception {
                return new Tuple5<>(GeoUtils.getGridCellCenterLon(filteredRide.f0), GeoUtils.getGridCellCenterLat(filteredRide.f0), filteredRide.f1, filteredRide.f2, filteredRide.f3);
            }
        });

        popularPlacesStream.print();

        Map<String, String> config = Maps.newHashMap();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "flink-tutorial");

        List<InetSocketAddress> elasticSearchTransportAddresses = Lists.newArrayList(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        popularPlacesStream.addSink(new ElasticsearchSink<>(config, elasticSearchTransportAddresses, new PopularPlacesElasticSearchSink()));

        executionEnvironment.execute();
    }

}
