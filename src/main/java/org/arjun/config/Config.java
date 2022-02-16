package org.arjun.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.StringSerializer;


import org.apache.kafka.streams.state.Stores;
import org.arjun.generator.TimeSeriesGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Configuration
public class Config {

    public static final String TM_TOPIC = "TM_TOPIC";
    public static final String V_5 = "33553442";
    public static final String SUM_TOPIC = "sum_topic";


    @Bean
    public DefaultKafkaProducerFactory<String, Long> timeSeriesFactory() {
        Map<String, Object> all = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
//                SECURITY_PROTOCOL_CONFIG, SASL_SSL,
//                SaslConfigs.SASL_JAAS_CONFIG, AUTH,
//                SaslConfigs.SASL_MECHANISM, PLAIN,
                CLIENT_ID_CONFIG, "timeSeriesProducer",
                BUFFER_MEMORY_CONFIG, V_5,
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                ACKS_CONFIG, "all");
        Map<String, Object> bootstrapServersConfig = new HashMap<>(all);
        return new DefaultKafkaProducerFactory<>(bootstrapServersConfig);
    }

    @Bean("timeSeriesTemplate")
    public KafkaTemplate<String, Long> timeSeriesTemplate(@Autowired @Qualifier("timeSeriesFactory") DefaultKafkaProducerFactory<String, Long> kafkaProducerFactory) {
        return new KafkaTemplate(kafkaProducerFactory);
    }

    @Bean
    public KafkaStreams timeSeriesStream() {
        Map<String, Object> timeSeriesProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                GROUP_ID_CONFIG, "timeSeries_consumer",
                StreamsConfig.APPLICATION_ID_CONFIG, "timeseries_stream",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000,
                SESSION_TIMEOUT_MS_CONFIG, 15000,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName(),
                StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams"
        );
        Properties properties = new Properties();
        for (String key : timeSeriesProps.keySet()) {
            properties.put(key, timeSeriesProps.get(key));
        }
        return new KafkaStreams(getTopology(), properties);
    }

    static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> input = builder.stream(TM_TOPIC);

//        final KTable<String, Long> sumOfOddNumbers = input.
//                filter((k, v) -> v % 2 != 0)
//                .selectKey((k, v) -> "1")
//                .groupByKey()
//                .reduce(Long::sum);

        var store = Stores.persistentTimestampedWindowStore(
                "some-state-store",
                Duration.ofMinutes(5),
                Duration.ofMinutes(2),
                false);
        var materialized = Materialized
                .<String, Long>as(store)
                .withKeySerde(Serdes.String());

//        List<Long> countSum = new ArrayList<>();
//        KTable<Windowed<String>, Long> count = input
//                .selectKey((k, v) -> "1")
//                .groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(9), Duration.ofMillis(1)))
//                .count(materialized);
//
//        count.toStream().foreach((key, value) -> {
//            countSum.add(value);
//            System.out.println(value);
//            Long total = 0l;
//            for (Long l: countSum) {
//                total = total + l;
//            }
//            System.out.println("Total timestamps having 10 millisecond diff "+total);
//        });
//
//        count.toStream().to(SUM_TOPIC);


        KTable<Windowed<String>, Long> reduce = input
                .selectKey((k, v) -> "1")
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(9), Duration.ofMillis(1)))
                .reduce((value1, value2) -> value1, materialized);
        KStream<Windowed<String>, Long> windowedLongKStream = reduce.toStream();
        windowedLongKStream.to(SUM_TOPIC);

//        sumOfOddNumbers.toStream().foreach((key, value) -> System.out.println(value));

¸
        return builder.build();
    }

    @Bean
    public TimeSeriesGenerator uccGenerator() {
        return new TimeSeriesGenerator();
    }

    @Bean
    NewTopic timeSeriesTopic() {
        return TopicBuilder.name(TM_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sumTopic() {
        return TopicBuilder.name(SUM_TOPIC).partitions(1).replicas(1).build();
    }

}
