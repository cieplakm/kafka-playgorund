package com.mmc.kafkaplayground.apps.temp.currenttemp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

record SensorCurrentTempEvent(BigDecimal currentTemp, LocalDateTime timestamp) {

}
