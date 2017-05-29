package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.StoppedSegment;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Implementation of the Connected Car Segments exercise
 * {@see http://dataartisans.github.io/flink-training/exercises/carSegments.html}
 *
 * Command line arguments:
 *  - file: the connected car events csv which should be processed
 *  - inOrder: true if the events in the provided files are in order, otherwise false
 */
public class ConnectedCarSegmentsExercise {

    private static final String FILE_PARAMETER = "file";
    private static final String ORDER_PARAMETER = "inOrder";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamExecutionEnvironment.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String file = parameterTool.get(FILE_PARAMETER);
        boolean inOrder = parameterTool.getBoolean(ORDER_PARAMETER);

        streamExecutionEnvironment
                .readTextFile(file)
                .map(new MapFunction<String, ConnectedCarEvent>() {
                    @Override
                    public ConnectedCarEvent map(String line) throws Exception {
                        return ConnectedCarEvent.fromString(line);
                    }
                })
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner())
                .windowAll(GlobalWindows.create())
                .trigger(new ConnectedCarEventWindowTrigger(inOrder))
                .apply(new AllWindowFunction<ConnectedCarEvent, StoppedSegment, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<ConnectedCarEvent> values, Collector<StoppedSegment> out) throws Exception {
                        out.collect(new StoppedSegment(values));
                    }
                })
                /* Filter out segments only consisting of the stop event itself */
                .filter(new FilterFunction<StoppedSegment>() {
                    @Override
                    public boolean filter(StoppedSegment stoppedSegment) throws Exception {
                        return stoppedSegment.length > 0;
                    }
                }).printToErr();

        streamExecutionEnvironment.execute("Gap Segments");
    }

    private static class ConnectedCarEventWindowTrigger extends Trigger<ConnectedCarEvent, Window> {

        private final boolean inOrder;

        ConnectedCarEventWindowTrigger(boolean inOrder) {
            this.inOrder = inOrder;
        }

        @Override
        public TriggerResult onElement(ConnectedCarEvent element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            if (element.speed == 0.0) {
                if (inOrder) {
                    return TriggerResult.FIRE_AND_PURGE;
                }
                ctx.registerEventTimeTimer(element.timestamp);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            if (inOrder) {
                return TriggerResult.CONTINUE;
            }
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {
            // cleanup any Trigger state
        }
    }
}
