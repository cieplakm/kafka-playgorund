package com.mmc.kafkaplayground.apps.temp.alert;

import com.mmc.kafkaplayground.apps.temp.alert.model.AlertEvent;
import com.mmc.kafkaplayground.apps.temp.alert.model.AlertsAggregator;
import com.mmc.kafkaplayground.apps.temp.currenttemp.TempAggregator;
import com.mmc.kafkaplayground.apps.temp.model.SensorTemperatureData;
import com.mmc.kafkaplayground.apps.temp.alert.model.TempAlertsAggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Objects;

@Slf4j
class IntervalProcessor implements Processor<String, TempAlertsAggregate, String, AlertEvent> {

    KeyValueStore<String, Long> store;
    private ProcessorContext<String, AlertEvent> context;


    @Override
    public void init(ProcessorContext<String, AlertEvent> context) {
        Processor.super.init(context);
        this.store = context.getStateStore("alerts-timestamps");
        this.context = context;
    }

    @Override
    public void process(Record<String, TempAlertsAggregate> record) {
        AlertsAggregator aggregator = record.value().alertsAggregator();
        TempAggregator sensorTemperatureData = record.value().sensorTemperatureData();
        aggregator.usersAlerts()
                .stream()
                .flatMap(userAlerts -> userAlerts.alerts()
                        .stream()
                        .map(alertConfiguration -> {
                            if (alertConfiguration.canApplied(BigDecimal.valueOf(sensorTemperatureData.getCurrentValue()))) {
                                String lastAlertTimestampKey = userAlerts.userId() + userAlerts.sensorId() + alertConfiguration.name();

                                Long lastTimestamp = store.get(lastAlertTimestampKey);

                                boolean shouldSendAlert = lastTimestamp == null || (System.currentTimeMillis() - lastTimestamp) > alertConfiguration.interval();

                                if (shouldSendAlert) {
                                    log.info("Sending alert!");
                                    store.put(lastAlertTimestampKey, System.currentTimeMillis());
                                    return new Record<>(record.key(),
                                            new AlertEvent(userAlerts.userId(),
                                                    alertConfiguration.email(),
                                                    alertConfiguration.name(),
                                                    BigDecimal.valueOf(sensorTemperatureData.getCurrentValue()),
                                                    sensorTemperatureData.getTimestamp(),
                                                    alertConfiguration.message()),
                                            sensorTemperatureData.getTimestamp().toInstant(ZoneOffset.UTC).toEpochMilli());
                                }
                            }
                            return null;

                        }))
                .filter(Objects::nonNull)
                .forEach(event -> {
                    context.forward(event);
                });
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
