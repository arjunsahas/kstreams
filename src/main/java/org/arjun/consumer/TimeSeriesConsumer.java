package org.arjun.consumer;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class TimeSeriesConsumer {

    @Autowired
    private KafkaStreams kafkaStreams;

    @EventListener(ApplicationStartedEvent.class)
    public void consume() {
//        kafkaStreams.cleanUp();
        if (!kafkaStreams.state().isRunningOrRebalancing())
            kafkaStreams.start();
    }

    @PreDestroy
    public void destroy() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
