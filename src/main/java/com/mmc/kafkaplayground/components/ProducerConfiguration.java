package com.mmc.kafkaplayground.components;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
class ProducerConfiguration {

    @Bean
    TransactionalKafkaProducer kafkaProducerClient() {
        return new TransactionalKafkaProducer(kafkaConfig());
    }

    private static Properties kafkaConfig() {
        Properties props = new Properties();

        double random = Math.random();
        //required props
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //optional props
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer_" + random);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tid");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }
}
