package com.mmc.kafkaplayground.apps.temp.alert.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record AlertEvent(String targetId, String email, String name, BigDecimal currentTemp, LocalDateTime timestamp, String message) {
}
