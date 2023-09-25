package com.mmc.kafkaplayground.components;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class TransactionalKafkaProducer {

    private final Producer<String, String> producer;

    public TransactionalKafkaProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
        this.producer.initTransactions();
    }

    public void produce(ProducerRecord<String, String> record) {
        producer.beginTransaction();
        Future<RecordMetadata> send = producer.send(record);
        try {
            RecordMetadata metadata = send.get(10000, TimeUnit.MILLISECONDS);
            log.info("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        producer.commitTransaction();
    }
}