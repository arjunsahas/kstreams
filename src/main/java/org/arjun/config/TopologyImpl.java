package org.arjun.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.arjun.config.Config.SUM_TOPIC;

/**
 * Sliding Window Vs Hopping Window,
 *  For Hopping window, window is created/reevaluated at fixed intervals independent of the incoming data.
 *  For Sliding window, window is re-evaluated only if the content of the window changes.
 */
public class TopologyImpl {

    public static void startTumblingWindow(final KStream<String, Long> input) {
        // Tumbling Window -  one event per window
        KTable<Windowed<String>, Long> tumblingWindowKTable = input
                .selectKey((k, v) -> "1")
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(9), Duration.ofMillis(1)))
                .reduce((value1, value2) -> value1, getMaterialisedView());
        tumblingWindowKTable.toStream().to(SUM_TOPIC);
        print(tumblingWindowKTable.toStream());
    }

    public static void startSlidingWindow(final KStream<String, Long> input) {
//        Sliding Window ->  one event per window
        KTable<Windowed<String>, Long> slidingWindowKTable = input
                .selectKey((k, v) -> "1")
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(9), Duration.ofMillis(1)))
                .reduce((value1, value2) -> value1, getMaterialisedView());
        slidingWindowKTable.toStream().to(SUM_TOPIC);
        print(slidingWindowKTable.toStream());
    }

    public static void startHoppingWindow(final KStream<String, Long> input) {
        // Hopping Window
        KTable<Windowed<String>, Long> hoppingWindowKTable = input
                .selectKey((k, v) -> "1")
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(9)).advanceBy(Duration.ofMillis(1)))
                .reduce((value1, value2) -> value1, getMaterialisedView());
        hoppingWindowKTable.toStream().to(SUM_TOPIC);
        print(hoppingWindowKTable.toStream());
    }

    public static void startSlidingWindowCountEvents(final KStream<String, Long> input) {
        // Sliding Window count the number of events
        List<Long> countSum = new ArrayList<>();
        KTable<Windowed<String>, Long> count = input
                .selectKey((k, v) -> "1")
                .groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(9), Duration.ofMillis(1)))
                .count(getMaterialisedView());

        count.toStream().foreach((key, value) -> {
            countSum.add(value);
            System.out.println(value);
            Long total = 0l;
            for (Long l : countSum) {
                total = total + l;
            }
            System.out.println("Total timestamps having 10 millisecond diff " + total);
        });

        count.toStream().to(SUM_TOPIC);
    }

    public static Materialized<String, Long, WindowStore<Bytes, byte[]>> getMaterialisedView() {
        var store = Stores.persistentTimestampedWindowStore(
                "some-state-store",
                Duration.ofMinutes(5),
                Duration.ofMinutes(2),
                false);
        return Materialized
                .<String, Long>as(store)
                .withKeySerde(Serdes.String());
    }

    private static void print(KStream<Windowed<String>, Long> windowedLongKStream) {
        windowedLongKStream.foreach((key, value) -> System.out.println(value));
    }

}
