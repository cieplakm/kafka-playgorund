package com.mmc.kafkaplayground.components;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmc.kafkaplayground.apps.temp.alert.model.UserSensorAlerts;
import com.mmc.kafkaplayground.apps.temp.model.SensorTemperatureData;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController
@RequestMapping("temp")
@RequiredArgsConstructor
@Slf4j
class TempController {

    private final TransactionalKafkaProducer client;
    private final ObjectMapper objectMapper;

    @PostMapping
    @SneakyThrows
    void callTemp(@RequestBody SensorTemperatureData request) {
        long t1 = System.currentTimeMillis();
        client.produce(new ProducerRecord<>("temperatures",
                request.sensorId(),
                objectMapper.writeValueAsString(new SensorTemperatureData(request.sensorId(), request.t(), request.p(), LocalDateTime.now()))));
        long t2 = System.currentTimeMillis();
        log.info("Sensor data saved in {}ms.", (t2 - t1));
    }

    @PostMapping
    @RequestMapping("configuration")
    @SneakyThrows
    void callTemp(@RequestBody UserSensorAlerts request) {
        log.info(request.toString());
        client.produce(new ProducerRecord<>("alert-configurations",
                request.userId() + request.sensorId(),
                objectMapper.writeValueAsString(request)));
    }
}
