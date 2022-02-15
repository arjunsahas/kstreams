package org.arjun.serdeser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.arjun.model.TimeModel;

import java.io.IOException;
import java.util.Map;

public class TimeModelDeserializer implements Deserializer<TimeModel> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public TimeModel deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, TimeModel.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TimeModel deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }
}
