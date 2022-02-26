package org.arjun;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import static org.arjun.config.Config.SUM_TOPIC;

@SpringBootApplication
@EnableKafka
public class KStreams {

    public static void main(String[] args) {
        SpringApplication.run(KStreams.class);
        consumeOutput("localhost:9092");
    }

    private static void consumeOutput(final String bootstrapServers) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sum-lambda-example-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(SUM_TOPIC));
        while (true) {
            final ConsumerRecords<Long, Long> records =
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            for (final ConsumerRecord<Long, Long> record : records) {
                System.out.println("Time Generated is:" + new Date(record.value()));
            }
        }
    }
}
