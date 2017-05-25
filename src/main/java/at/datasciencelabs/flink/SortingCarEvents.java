package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The Sorting Car Events exercise of the Apache Flink training.
 * {@see http://dataartisans.github.io/flink-training/exercises/carSort.html}
 */
public class SortingCarEvents {

    private static final String CAR_ID_FIELD = "carId";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamExecutionEnvironment.setParallelism(1);

        ParameterTool paramterTools = ParameterTool.fromArgs(args);

        String file = paramterTools.get("file");

        streamExecutionEnvironment
                .readTextFile(file)
                .map(new MapFunction<String, ConnectedCarEvent>() {
                    @Override
                    public ConnectedCarEvent map(String line) throws Exception {
                        return ConnectedCarEvent.fromString(line);
                    }
                })
                /*
                  The {@link ConnectedCarAssigner assigns a watermark after every event, the result is
                 * that the timers we will register in {@link SortConnectedCarEventsFunction#processElement(ConnectedCarEvent, ProcessFunction.Context, Collector)}
                 * will also be evaluated after every event.
                 */
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner())
                .keyBy(CAR_ID_FIELD)
                .process(new SortConnectedCarEventsFunction())
                .print();

        streamExecutionEnvironment.execute("Sort Car Events");
    }

}
