package com.example.kafkastreams.consumer;

import com.example.kafkastreams.config.KafkaConfiguration;
import com.example.kafkastreams.serdes.CustomSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;

@Slf4j
@Component
@RequiredArgsConstructor
public class SampleConsumer {

    private final KafkaConfiguration kafkaConfiguration;

    //https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#streams-spring
    @Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream(kafkaConfiguration.getInputTopic(), Consumed.with(Serdes.String(), Serdes.String()));
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        SessionWindows sessionWindow = SessionWindows.ofInactivityGapAndGrace(windowSize, gracePeriod);

        stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(sessionWindow)
                .aggregate(ArrayList::new,
                        (key, value, aggregate1) -> { aggregate1.add(value); return aggregate1; },
                        (key, aggregate1, aggregate2) -> { aggregate1.addAll(aggregate2); return aggregate1; },
                        Materialized.with(Serdes.String(), CustomSerdes.MessageList()))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value))
                .to(kafkaConfiguration.getOutputTopic(), Produced.with(Serdes.String(), CustomSerdes.MessageList()));

        return stream;
    }
}