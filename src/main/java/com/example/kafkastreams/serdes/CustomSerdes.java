package com.example.kafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.ArrayList;

public final class CustomSerdes {

    public static Serde<ArrayList> MessageList() {
        JsonSerializer<ArrayList> serializer = new JsonSerializer<>();
        JsonDeserializer<ArrayList> deserializer = new JsonDeserializer<>(ArrayList.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}