package org.arjun.serdeser;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.arjun.model.TimeModel;

public final class TimeModelSerdes {
    private TimeModelSerdes() {
    }

    public static Serde<TimeModel> timeModelSerdes() {
        TimeModelSerializer serializer = new TimeModelSerializer();
        TimeModelDeserializer deserializer = new TimeModelDeserializer();
        return Serdes.serdeFrom(serializer, deserializer);
    }
}