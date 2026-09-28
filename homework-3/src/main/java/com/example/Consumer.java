package com.example;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class Consumer {
    public static void main(String[] args) {
        String isolation = args.length > 0 ? args[0] : "read_committed";
        System.out.println("[INIT] consumer started, isolation.level = " + isolation);

        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "tx-consumer-" + System.currentTimeMillis());
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("auto.offset.reset", "earliest");
        p.put("isolation.level", isolation);

        p.put("security.protocol", "SASL_PLAINTEXT");
        p.put("sasl.mechanism", "PLAIN");
        p.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"admin\" password=\"admin-secret\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(Arrays.asList("topic1", "topic2"));
        System.out.println("[SUBSCRIBE] subscribed to topic1, topic2");
        System.out.println();

        long deadline = System.currentTimeMillis() + 10000;
        int total = 0;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                System.out.printf("[READ]  %s -> %s%n", r.topic(), r.value());
                total++;
            }
            if (total >= 14) break;
        }

        System.out.println();
        System.out.printf("[TOTAL] read %d messages with isolation.level=%s%n", total, isolation);
        consumer.close();
    }
}
