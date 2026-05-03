package com.unifieddataprocessing.pubsub.kafka;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;

import java.time.Duration;
import java.util.List;

/**
 * Kafka-backed {@link PubSubConsumer}. Stub: the real implementation will
 * delegate to {@code org.apache.kafka.clients.consumer.KafkaConsumer} once
 * the kafka-clients dependency is added.
 */
public class KafkaConsumerStub implements PubSubConsumer {

    @Override
    public void connect() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void subscribe(String topic) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void unsubscribe(String topic) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<Message> poll(Duration timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void acknowledge(Message message) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
