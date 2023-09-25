package com.mmc.kafkaplayground.apps.temp;

import com.mmc.kafka.KafkaConsumerBuilder;
import com.mmc.kafkaplayground.App;
import com.mmc.kafkaplayground.components.emailsender.EmailSender;
import com.mmc.kafkaplayground.apps.temp.alert.model.AlertEvent;
import com.mmc.kafkaplayground.kafka.streams.JsonSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
@RequiredArgsConstructor
@Slf4j
class AlertSenderConsumerApp implements App {

    private final EmailSender emailSender;
    private KafkaConsumer<String, String> consumer;

    @Override
    public void run() {
        consumer = KafkaConsumerBuilder.<String, String>instance("temperature-alert-consumer")
                .readCommitted()
                .disableAutoCommit()
                .readFromTheEnd()
                .pullMessagesAtOnce(1)
                .withInstanceId("consumer_1")
                .withStringValueDeserializer()
                .withStringKeyDeserializer()
                .build();

        consumer.subscribe(Collections.singletonList("temperature-alerts"));

        while (true) {
            try {
                listenForAlerts();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void listenForAlerts() throws IOException {
        log.info("Listening for alerts...");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

        for (ConsumerRecord<String, String> record : records) {
            AlertEvent deserialize = JsonSerdes.forClass(AlertEvent.class).deserializer().deserialize(record.topic(), record.value().getBytes(StandardCharsets.UTF_8));

            String subject = deserialize.name() + " from sensor " + record.key();
            String message = String.format("""
                    %s,
                    %s *C,
                    %s
                    """, deserialize.message(), deserialize.currentTemp(), deserialize.timestamp());

            emailSender.sendEmail(deserialize.email(), subject, message);
        }
        consumer.commitSync();
    }

//    private static Properties kafkaConfig() {
//        String groupId = "temperature-alert-consumer";
//        String isolation = "read_committed";
//        String autoCommit = "false";
//        String consumerId = groupId;
//
//        Properties props = new Properties();
//        // required props
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        // optional props
//        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, consumerId);
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
//
//        return props;
//    }
}