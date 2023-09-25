package com.mmc.kafkaplayground.apps.temp.currenttemp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class TempAggregator {

    Double currentValue;
    LocalDateTime timestamp;

    public void aggregate(BigDecimal currentTemp, LocalDateTime timestamp) {
        this.timestamp = timestamp;
        if (currentValue == null) {
            currentValue = currentTemp.doubleValue();

            return;
        } else {
            currentValue = (currentValue + currentTemp.doubleValue()) / 2;
        }
    }
}
