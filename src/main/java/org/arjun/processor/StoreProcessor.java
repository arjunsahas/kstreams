package org.arjun.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Date;

public class StoreProcessor implements Processor<String, Long, String, Long> {

    private ProcessorContext context;
    KeyValueStore<String, Long> kvStore;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        this.kvStore = context.getStateStore("custom-time-series-store");
//        this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, new SamplePunctuator(kvStore, context));
        Processor.super.init(context);
    }

    @Override
    public void process(Record<String, Long> record) {
        kvStore.putIfAbsent(record.key(), record.value());
        System.out.println("Processor: Record Successfully Processed " + new Date(record.value()));
//        context.forward(record);
    }
}
