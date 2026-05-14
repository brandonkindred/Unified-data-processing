package com.unifieddataprocessing.pubsub.relay;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RelayRegistrationTest {

  private static final PubSubConsumer CONSUMER = new NoOpConsumer();
  private static final PubSubPublisher PUBLISHER = new NoOpPublisher();

  @Test
  void validRegistration_constructsAndExposesFields() {
    RelayRegistration reg =
        new RelayRegistration("rabbit-prod", "shopify.orders", "orders_q", CONSUMER, PUBLISHER);
    assertEquals("rabbit-prod", reg.destinationId());
    assertEquals("shopify.orders", reg.sourceTopic());
    assertEquals("orders_q", reg.downstreamTopic());
    assertSame(CONSUMER, reg.consumer());
    assertSame(PUBLISHER, reg.publisher());
  }

  @Test
  void destinationId_withDot_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit.prod", "src", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void destinationId_withSlash_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit/prod", "src", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void sourceTopic_withDot_accepted() {
    assertDoesNotThrow(
        () -> new RelayRegistration("rabbit", "shopify.orders", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void downstreamTopic_withSlash_accepted() {
    // Downstream brokers (Pulsar, Kinesis, GCP, etc.) have varied naming rules; the relay does
    // not constrain the topic format on the destination side.
    assertDoesNotThrow(
        () -> new RelayRegistration("rabbit", "src", "tenant/ns/topic", CONSUMER, PUBLISHER));
  }

  @Test
  void nullDestinationId_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration(null, "src", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void blankDestinationId_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("  ", "src", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void nullSourceTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", null, "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void blankSourceTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", "  ", "dst", CONSUMER, PUBLISHER));
  }

  @Test
  void nullDownstreamTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", "src", null, CONSUMER, PUBLISHER));
  }

  @Test
  void blankDownstreamTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", "src", "  ", CONSUMER, PUBLISHER));
  }

  @Test
  void nullConsumer_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", "src", "dst", null, PUBLISHER));
  }

  @Test
  void nullPublisher_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayRegistration("rabbit", "src", "dst", CONSUMER, null));
  }

  private static final class NoOpConsumer implements PubSubConsumer {
    @Override
    public void connect() {}

    @Override
    public void subscribe(String topic) {}

    @Override
    public void unsubscribe(String topic) {}

    @Override
    public List<Message> poll(Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public void acknowledge(Message message) {}

    @Override
    public void close() {}
  }

  private static final class NoOpPublisher implements PubSubPublisher {
    @Override
    public void connect() {}

    @Override
    public CompletableFuture<PublishResult> publish(Message message) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public PublishResult publishSync(Message message) {
      return null;
    }

    @Override
    public CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
