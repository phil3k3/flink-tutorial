package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.CompareByTimestampAscending;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

/**
 * State-ful function which uses a {@link PriorityQueue} to sort the out of order events. As soon
 * as the register timer's expire, the events in the queue are evaluated against the current watermark
 * and released as soon as the watermark gets high enough for them to be released.
 */
class SortConnectedCarEventsFunction extends RichProcessFunction<ConnectedCarEvent, ConnectedCarEvent> {

    private static final String EVENT_QUEUE = "eventQueue";
    private transient ValueState<PriorityQueue<ConnectedCarEvent>> eventQueue;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor = new ValueStateDescriptor<>(
                EVENT_QUEUE,
                TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>() {
                })
        );
        eventQueue = getRuntimeContext().getState(descriptor);
    }

    /**
     * Register an Event Time timer for the current event's timestamp. This timer will expire as soon as the watermark
     * passes the corresponding timestamp defined in the event.
     */
    @Override
    public void processElement(ConnectedCarEvent connectedCarEvent, Context context, Collector<ConnectedCarEvent> collector) throws Exception {

        TimerService timerService = context.timerService();

        Long currentTimestamp = context.timestamp();
        Long currentWatermark = timerService.currentWatermark();

        if (currentTimestamp > currentWatermark) {
            PriorityQueue<ConnectedCarEvent> priorityQueue = new PriorityQueue<>(10, new CompareByTimestampAscending());
            if (eventQueue.value() != null) {
                priorityQueue = eventQueue.value();
            }
            priorityQueue.add(connectedCarEvent);
            eventQueue.update(priorityQueue);
            timerService.registerEventTimeTimer(connectedCarEvent.timestamp);
        }
    }

    /**
     * Evaluates all (already sorted) events in the  queue and emits those smaller than the current watermark
     */
    @Override
    public void onTimer(long l, OnTimerContext onTimerContext, Collector<ConnectedCarEvent> collector) throws Exception {
        PriorityQueue<ConnectedCarEvent> priorityQueue = eventQueue.value();
        Long watermark = onTimerContext.timerService().currentWatermark();
        ConnectedCarEvent connectedCarEvent = priorityQueue.peek();

        while (connectedCarEvent != null && connectedCarEvent.timestamp <= watermark) {
            collector.collect(connectedCarEvent);
            priorityQueue.remove(connectedCarEvent);
            connectedCarEvent = priorityQueue.peek();
        }
    }
}
