package com.kafka.training.app;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_COUNT_TOPIC = "PERSON_COUNT_TOPIC";
    public static final String GROUP_PERSON_BY_NATIONALITY_TOPIC =
        "GROUP_PERSON_BY_NATIONALITY_TOPIC";
    public static final String PERSON_COUNT_STORE = "PERSON_COUNT_STORE";

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, groups by nationality and
     * counts the number of persons.
     * The result is written to the PERSON_COUNT_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        // Create a custom Serde for KafkaPerson using JSON
        Serde<Person> personSerde = new JsonSerde<>(Person.class);

        streamsBuilder
                .<String, Person>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), personSerde))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getNationality().toString(),
                        Grouped.with(GROUP_PERSON_BY_NATIONALITY_TOPIC, Serdes.String(), personSerde))
                .count(Materialized
                        .<String, Long, KeyValueStore<Bytes, byte[]>>as(PERSON_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to(PERSON_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
