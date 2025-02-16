package com.kafka.training.app.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import com.kafka.training.app.KafkaStreamsRunner;
import com.kafka.training.app.KafkaStreamsTopology;

@RestController
@RequestMapping("/api/persons")
@RequiredArgsConstructor
public class PersonCountController {

    private final KafkaStreamsRunner kafkaStreamsRunner;

    @GetMapping("/count/{nationality}")
    public Long getCountByNationality(@PathVariable String nationality) {
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
            kafkaStreamsRunner.getKafkaStreams().store(
                StoreQueryParameters.fromNameAndType(
                    KafkaStreamsTopology.PERSON_COUNT_STORE,
                    QueryableStoreTypes.keyValueStore()
                )
            );
        
        return keyValueStore.get(nationality);
    }

    @GetMapping("/count")
    public Long getTotalCount() {
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
            kafkaStreamsRunner.getKafkaStreams().store(
                StoreQueryParameters.fromNameAndType(
                    KafkaStreamsTopology.PERSON_COUNT_STORE,
                    QueryableStoreTypes.keyValueStore()
                )
            );

        long total = 0;
        try (var iterator = keyValueStore.all()) {
            while (iterator.hasNext()) {
                total += iterator.next().value;
            }
        }
        return total;
    }

    @PutMapping("/count/{nationality}")
    public void updateCountByNationality(@PathVariable String nationality, @RequestBody Long count) {
        ReadOnlyKeyValueStore<String, Long> readOnlyStore =
            kafkaStreamsRunner.getKafkaStreams().store(
                StoreQueryParameters.fromNameAndType(
                    KafkaStreamsTopology.PERSON_COUNT_STORE,
                    QueryableStoreTypes.keyValueStore()
                )
            );
            
        if (readOnlyStore instanceof KeyValueStore) {
            KeyValueStore<String, Long> keyValueStore = (KeyValueStore<String, Long>) readOnlyStore;
            keyValueStore.put(nationality, count);
        } else {
            throw new IllegalStateException("Store is not writable");
        }
    }
}