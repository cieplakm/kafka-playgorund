package com.mmc.kafkaplayground.apps.temp.alert.model;

import java.util.Set;

public record AlertsAggregator(Set<UserSensorAlerts> usersAlerts) {

    public void add(UserSensorAlerts alert) {
        usersAlerts.remove(alert);
        usersAlerts.add(alert);
    }
}
