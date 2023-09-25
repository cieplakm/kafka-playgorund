package com.mmc.kafkaplayground.kafka.streams;

import com.mmc.kafkaplayground.App;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

@Slf4j
public abstract class KafkaStreamApp implements App {

    public void run() {
        Properties config = config();
        Topology topology = topology();

        log.info(topology.describe().toString());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);

        kafkaStreams.setUncaughtExceptionHandler(e -> {
            log.error(e.getMessage(), e);
            return null;
        });

        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("Kafka Streams application {} has benn successfully run.", config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    protected abstract String appName();

    protected abstract Topology topology();

    protected Properties config() {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

        return config;
    }
}
