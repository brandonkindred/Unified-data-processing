package com.unifieddataprocessing.pubsub;

import java.time.Duration;
import java.util.List;

/**
 * Placeholder implementation of {@link PubSubConsumer}. Concrete connectors
 * (Kafka, Google Cloud Pub/Sub, AWS SNS/SQS, RabbitMQ) will be added as
 * separate implementations of {@link PubSubConsumer}.
 */
public class PubSubConsumerStub implements PubSubConsumer {

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
