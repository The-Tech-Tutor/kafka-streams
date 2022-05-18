package com.example.kafkastreams.consumer;

import com.example.kafkastreams.config.KafkaConfiguration;
import com.example.kafkastreams.serdes.CustomSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SampleConsumer {

    private final KafkaConfiguration kafkaConfiguration;

    //https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#streams-spring
    @Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream(kafkaConfiguration.getInputTopic());
        Duration windowSize = Duration.ofSeconds(5);
        TimeWindows tumblingWindow = TimeWindows.of(windowSize);

        stream.groupByKey()
                .windowedBy(tumblingWindow)
                .aggregate(ArrayList::new, (k, v, vr) -> {
                    vr.add(v);
                    return vr;
                }, Materialized.with(Serdes.String(), CustomSerdes.MessageList()))
        .toStream().to("output");

        return stream;
    }
}
