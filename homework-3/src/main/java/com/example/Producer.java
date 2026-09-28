package com.example;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("transactional.id", "tx-" + System.currentTimeMillis());

        p.put("security.protocol", "SASL_PLAINTEXT");
        p.put("sasl.mechanism", "PLAIN");
        p.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"admin\" password=\"admin-secret\";");

        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.initTransactions();
        System.out.println("[INIT] producer initialized, transactions ready");

        System.out.println();
        System.out.println("=== TRANSACTION 1 ===");
        producer.beginTransaction();
        System.out.println("[BEGIN]  transaction 1 started");
        for (int i = 1; i <= 5; i++) {
            producer.send(new ProducerRecord<>("topic1", "k" + i, "committed-" + i));
            producer.send(new ProducerRecord<>("topic2", "k" + i, "committed-" + i));
            System.out.println("[SEND]   topic1 <- committed-" + i + " | topic2 <- committed-" + i);
        }
        producer.commitTransaction();
        System.out.println("[COMMIT] transaction 1 committed (10 messages total)");

        System.out.println();
        System.out.println("=== TRANSACTION 2 ===");
        producer.beginTransaction();
        System.out.println("[BEGIN]  transaction 2 started");
        for (int i = 1; i <= 2; i++) {
            producer.send(new ProducerRecord<>("topic1", "k" + i, "aborted-" + i));
            producer.send(new ProducerRecord<>("topic2", "k" + i, "aborted-" + i));
            System.out.println("[SEND]   topic1 <- aborted-" + i + " | topic2 <- aborted-" + i);
        }

        System.out.println("[FLUSH]  flushing pending records to broker...");
        producer.flush();
        producer.abortTransaction();
        System.out.println("[ABORT]  transaction 2 aborted (4 messages written to log, marked aborted)");

        producer.close();
        System.out.println();
        System.out.println("[DONE]   producer closed");
    }
}
