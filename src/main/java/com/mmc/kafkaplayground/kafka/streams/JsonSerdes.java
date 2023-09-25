package com.mmc.kafkaplayground.kafka.streams;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class JsonSerdes {

    public static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        objectMapper.setDateFormat(dateFormat);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    public static <T> Serde<T> forClass(Class<? extends T> clazz) {
        return Serdes.serdeFrom(getObjectSerializer(), getTemMaxAggregatorDeserializer(clazz));
    }

    public static <T> Deserializer<T> getTemMaxAggregatorDeserializer(Class<? extends T> clazz) {
        return (s, bytes) -> {
            try {
                return objectMapper.readValue(bytes, clazz);
            } catch (IOException e) {

                throw new RuntimeException(e + " message - " + new String(bytes));
            }
        };
    }

    private static <T> Serializer<T> getObjectSerializer() {
        return (s, o) -> {
            try {
                return objectMapper.writeValueAsBytes(o);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
