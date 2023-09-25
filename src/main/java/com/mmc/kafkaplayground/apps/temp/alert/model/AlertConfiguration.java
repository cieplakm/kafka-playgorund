package com.mmc.kafkaplayground.apps.temp.alert.model;

import java.math.BigDecimal;

public record AlertConfiguration(String name,
                                 BigDecimal tempFrom,
                                 BigDecimal tempTo,
                                 Long interval,
                                 String message,
                                 String email) {
    public boolean canApplied(BigDecimal currentTemp) {
        return currentTemp.compareTo(tempFrom()) >= 0 && currentTemp.compareTo(tempTo()) <= 0;
    }
}
