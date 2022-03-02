package org.arjun.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Date;

public class FutureTimePunctuator implements Punctuator {
    private KeyValueStore<String, Long> kvStore;
    private ProcessorContext context;


    FutureTimePunctuator(KeyValueStore<String, Long> kvStore, ProcessorContext context) {
        this.kvStore = kvStore;
        this.context = context;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Long> all = kvStore.all();
        while (all.hasNext()) {
            KeyValue<String, Long> next = all.next();
            // compare the incoming epoch with the clock time. if time is before ignore else process
            Date incomingTime = new Date(next.value);

            System.out.println("============================");
            System.out.println("Incoming Time:" + incomingTime);
            System.out.println("Current Time:" + new Date(timestamp));

            if (incomingTime.after(new Date(timestamp))) {
                System.out.println("Skipping ..");
            } else {
                System.out.println("Accepted Time:" + incomingTime);
                context.forward(new Record(next.key, next.value, timestamp));
                context.commit();
            }
        }
        all.close();
    }
}
