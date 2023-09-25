package com.mmc.kafkaplayground.apps.temp.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;


public record SensorTemperatureData(String sensorId, BigDecimal t, BigDecimal p, LocalDateTime timestamp) {
}
