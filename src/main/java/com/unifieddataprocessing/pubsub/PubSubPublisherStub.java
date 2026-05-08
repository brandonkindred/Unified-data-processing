package com.unifieddataprocessing.pubsub;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Placeholder implementation of {@link PubSubPublisher}. Concrete connectors (Kafka, GCP Pub/Sub,
 * Kinesis, Pulsar, MSK) live in their own subpackages.
 */
public class PubSubPublisherStub implements PubSubPublisher {

  @Override
  public void connect() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public CompletableFuture<PublishResult> publish(Message message) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public PublishResult publishSync(Message message) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
