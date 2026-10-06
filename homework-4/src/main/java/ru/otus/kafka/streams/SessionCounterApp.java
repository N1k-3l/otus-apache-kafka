package ru.otus.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SessionCounterApp {
    private static final Logger log = LoggerFactory.getLogger(SessionCounterApp.class);

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-counter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");

        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();
        Duration inactivityGap = Duration.ofMinutes(5);

        builder.stream("events", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, val) -> log.info("Получено событие -> Ключ: {}, Значение: {}", key, val))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("session-counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .filter((windowedKey, count) -> count != null)
                .map((windowedKey, count) -> {
                    String cleanKey = windowedKey.key();
                    String info = "Ключ: " + cleanKey + " | Событий в сессии: " + count;
                    log.info("Отправка в вывод: {}", info);
                    return new KeyValue<>(cleanKey, String.valueOf(count));
                })
                .to("events-aggregated", Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
            log.info("Приложение Kafka Streams успешно остановлено.");
        }));

        try {
            log.info("Запуск приложения Kafka Streams...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            log.error("Критическая ошибка при работе приложения:", e);
            System.exit(1);
        }
        System.exit(0);
    }
}
