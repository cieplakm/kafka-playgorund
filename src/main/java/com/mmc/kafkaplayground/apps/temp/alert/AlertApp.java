package com.mmc.kafkaplayground.apps.temp.alert;

import com.mmc.kafkaplayground.apps.temp.alert.model.AlertEvent;
import com.mmc.kafkaplayground.apps.temp.alert.model.AlertsAggregator;
import com.mmc.kafkaplayground.apps.temp.alert.model.TempAlertsAggregate;
import com.mmc.kafkaplayground.apps.temp.alert.model.UserSensorAlerts;
import com.mmc.kafkaplayground.apps.temp.currenttemp.TempAggregator;
import com.mmc.kafkaplayground.apps.temp.model.SensorTemperatureData;
import com.mmc.kafkaplayground.kafka.streams.JsonSerdes;
import com.mmc.kafkaplayground.kafka.streams.KafkaStreamApp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashSet;


@Component
class AlertApp extends KafkaStreamApp {

    @Override
    public String appName() {
        return "alert-app-XX";
    }

    protected Topology topology() {
        String sourceTopic = "temperatures";
        String configSourceTopic = "alert-configurations";
        String targetTopic = "temperature-alerts";
        String storeName = "alerts-timestamps";

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, Long>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                Serdes.Long()
        );

        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, UserSensorAlerts> userSensorAlerts = streamsBuilder.stream(configSourceTopic,
                        Consumed.with(Serdes.String(), JsonSerdes.forClass(UserSensorAlerts.class)))
                .toTable()
                .toStream();

        KTable<String, AlertsAggregator> sensorAlertConfigurations = userSensorAlerts.groupBy((key, value) -> value.sensorId(),
                        Grouped.with(Serdes.String(), JsonSerdes.forClass(UserSensorAlerts.class)))
                .aggregate(() -> new AlertsAggregator(new HashSet<>()),
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), JsonSerdes.forClass(AlertsAggregator.class)));

        KTable<Windowed<String>, TempAggregator> sensorData = streamsBuilder.stream(sourceTopic,
                        Consumed.with(Serdes.String(), JsonSerdes.forClass(SensorTemperatureData.class)))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .aggregate(TempAggregator::new, (key, value, aggregate) -> {
                    aggregate.aggregate(value.t(), value.timestamp());
                    return aggregate;
                }, Materialized.with(Serdes.String(), JsonSerdes.forClass(TempAggregator.class)));

        KStream<Windowed<String>, TempAggregator> windowedTempAggregatorKTable = sensorData.toStream();
        KTable<String, TempAggregator> map = windowedTempAggregatorKTable.map((key, value) -> new KeyValue<>(key.key(), value))
                .toTable(Materialized.with(Serdes.String(), JsonSerdes.forClass(TempAggregator.class)));


        KTable<String, TempAlertsAggregate> join = map.join(sensorAlertConfigurations, TempAlertsAggregate::new);

        join.toStream()
                .process(IntervalProcessor::new, storeName)
                .to(targetTopic, Produced.with(Serdes.String(), JsonSerdes.forClass(AlertEvent.class)));

        return streamsBuilder.build();
    }
}
