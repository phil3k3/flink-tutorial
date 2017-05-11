package at.datasciencelabs.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * Created by phil3k on 11.05.17.
 */
class TaxiRideBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

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
