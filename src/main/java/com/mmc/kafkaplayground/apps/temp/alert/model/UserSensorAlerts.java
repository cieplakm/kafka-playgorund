package com.mmc.kafkaplayground.apps.temp.alert.model;

import java.util.List;
import java.util.Objects;

public record UserSensorAlerts(String userId, String sensorId, List<AlertConfiguration> alerts) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserSensorAlerts that = (UserSensorAlerts) o;
        return Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId);
    }
}
