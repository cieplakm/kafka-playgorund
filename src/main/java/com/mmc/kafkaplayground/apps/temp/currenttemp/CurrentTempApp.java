package com.mmc.kafkaplayground.apps.temp.currenttemp;

import com.mmc.kafkaplayground.apps.temp.model.SensorTemperatureData;
import com.mmc.kafkaplayground.kafka.streams.JsonSerdes;
import com.mmc.kafkaplayground.kafka.streams.KafkaStreamApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;

//@Component
@Slf4j
class CurrentTempApp extends KafkaStreamApp {

    @Override
    public String appName() {
        return "current-temp-app";
    }

    protected Topology topology() {
        String sourceTopic = "temperatures";

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, SensorTemperatureData> tempDataStream = streamsBuilder.stream(sourceTopic,
                Consumed.with(Serdes.String(), JsonSerdes.forClass(SensorTemperatureData.class)));

        KTable<Windowed<String>, SensorCurrentTempEvent> currentTemp = tempDataStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.forClass(SensorTemperatureData.class)))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .aggregate(TempAggregator::new,
                        (key, temperature, aggregator) -> {
                            aggregator.aggregate(temperature.t(), temperature.timestamp());
                            return aggregator;
                        }, Materialized.with(Serdes.String(), JsonSerdes.forClass(TempAggregator.class)))
                .mapValues((readOnlyKey, value) -> new SensorCurrentTempEvent(BigDecimal.valueOf(value.getCurrentValue()), value.getTimestamp()));

        StoreBuilder<KeyValueStore<String, Long>> currentTempStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("sensor-current-temp-timestamps"),
                Serdes.String(),
                Serdes.Long()
        );

        streamsBuilder.addStateStore(currentTempStore);

        currentTemp.toStream()
                .process(() -> new SendEveryInterval(1, "sensor-current-temp-timestamps"), "sensor-current-temp-timestamps")
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .to("sensor-current-temp", Produced.with(Serdes.String(), JsonSerdes.forClass(SensorCurrentTempEvent.class)));

        return streamsBuilder.build();
    }
}
