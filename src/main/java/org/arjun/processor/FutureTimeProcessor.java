package org.arjun.processor;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Date;

public class FutureTimeProcessor implements Processor<String, Long, String, Long> {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        this.kvStore = context.getStateStore("custom-time-series-store");
        this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, new FutureTimePunctuator(kvStore, context));
        Processor.super.init(context);
    }

    @Override
    public void process(Record<String, Long> record) {
        System.out.println("Processor1: Record Successfully Processed " + new Date(record.value()));
//        context.forward(record);
    }
}
