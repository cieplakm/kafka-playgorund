package com.mmc.kafkaplayground.apps.temp.currenttemp;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

class SendEveryInterval implements Processor<Windowed<String>, SensorCurrentTempEvent, Windowed<String>, SensorCurrentTempEvent> {

    private final int min;
    private final String storeName;
    private KeyValueStore<String, Long> stateStore;
    private ProcessorContext<Windowed<String>, SensorCurrentTempEvent> context;

    public SendEveryInterval(int min, String storeName) {
        this.min = min;
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Windowed<String>, SensorCurrentTempEvent> context) {
        this.context = context;
        Processor.super.init(context);
        stateStore = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<Windowed<String>, SensorCurrentTempEvent> record) {
        String key = record.key().key();
        Long lastTimestamp = stateStore.get(key);

        if (lastTimestamp == null || ((System.currentTimeMillis() - lastTimestamp) > (long) min * 60 * 1000)) {
            context.forward(record);
            stateStore.put(key, System.currentTimeMillis());
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
