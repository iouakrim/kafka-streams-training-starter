package com.kafka.training.app;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * This class represents a Kafka Streams runner that runs a topology.
 */
@Slf4j
@Component
public class KafkaStreamsRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private KafkaStreamsProperties properties;

    private KafkaStreams kafkaStreams;

    /**
     * Starts the Kafka Streams when the application is ready.
     * The Kafka Streams topology is built in the {@link KafkaStreamsTopology} class.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        log.info("Starting streams");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        properties.getProperties().put(StreamsConfig.STATE_DIR_CONFIG, 
            "/tmp/kafka-streams-training/" + UUID.randomUUID());

        kafkaStreams = new KafkaStreams(topology, properties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                    properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    /**
     * Closes the Kafka Streams when the application is stopped.
     */
    @PreDestroy
    public void preDestroy() {
        log.info("Closing streams");
        kafkaStreams.close();
    }
}
