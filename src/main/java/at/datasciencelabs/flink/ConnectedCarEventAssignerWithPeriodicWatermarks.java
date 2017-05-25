package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

class ConnectedCarEventAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ConnectedCarEvent> {

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
