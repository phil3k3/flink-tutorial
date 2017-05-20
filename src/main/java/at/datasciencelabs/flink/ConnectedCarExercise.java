package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.GapSegment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class ConnectedCarExercise {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String file = parameterTool.get("file");
        final long timeLag = parameterTool.getLong("lag");

        executionEnvironment
                .readTextFile(file)
                .map(new MapFunction<String, ConnectedCarEvent>() {
                    @Override
                    public ConnectedCarEvent map(String line) throws Exception {
                        return ConnectedCarEvent.fromString(line);
                    }
                })
                .assignTimestampsAndWatermarks(new ConnectedCarEventAssignerWithPeriodicWatermarks(timeLag))
                .keyBy("carId")
                .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
                .apply(new GapSegmentTimeWindowAllWindowFunction()).print();

        executionEnvironment.execute("Connected Cars");
    }

    private static class ConnectedCarEventAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ConnectedCarEvent> {

        private final long timeLag;
        private long currentMaxTimestamp;

        ConnectedCarEventAssignerWithPeriodicWatermarks(long timeLag) {
            this.timeLag = timeLag;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - timeLag);
        }

        @Override
        public long extractTimestamp(ConnectedCarEvent connectedCarEvent, long previousElementTimestamp) {
            long timestamp = connectedCarEvent.timestamp;
            currentMaxTimestamp = Math.max(timestamp, previousElementTimestamp);
            return timestamp;
        }
    }

    private static class GapSegmentTimeWindowAllWindowFunction implements WindowFunction<ConnectedCarEvent, GapSegment, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectedCarEvent> iterable, Collector<GapSegment> collector) throws Exception {
            collector.collect(new GapSegment(iterable));
        }
    }
}
