package com.mmc.kafkaplayground.apps.temp.alert.model;

import com.mmc.kafkaplayground.apps.temp.currenttemp.TempAggregator;

public record TempAlertsAggregate(TempAggregator sensorTemperatureData, AlertsAggregator alertsAggregator) {

}
