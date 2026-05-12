package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class RegistrationTest {

  private static final PubSubConsumer CONSUMER = new NoOpConsumer();
  private static final ChannelOptions OPTIONS = ChannelOptions.defaults();

  @Test
  void validRegistration_constructsAndExposesFields() {
    Registration reg =
        new Registration(
            "shopify", "orders", "shopify-orders-sub", "shopify.orders", CONSUMER, OPTIONS);
    assertEquals("shopify", reg.sourceId());
    assertEquals("orders", reg.channel());
    assertEquals("shopify-orders-sub", reg.sourceTopic());
    assertEquals("shopify.orders", reg.targetTopic());
    assertSame(CONSUMER, reg.consumer());
    assertSame(OPTIONS, reg.options());
  }

  @Test
  void sourceId_withDot_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shop.ify", "orders", "src", "shop.ify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void sourceId_withSlash_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shop/ify", "orders", "src", "shop/ify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void channel_withDot_accepted() {
    assertDoesNotThrow(
        () ->
            new Registration(
                "shopify", "events.v2", "src", "shopify.events.v2", CONSUMER, OPTIONS));
  }

  @Test
  void channel_withSlash_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Registration(
                "shopify", "events/v2", "src", "shopify.events/v2", CONSUMER, OPTIONS));
  }

  @Test
  void combinedLength_249_accepted() {
    String channel = "c".repeat(247);
    assertDoesNotThrow(
        () -> new Registration("a", channel, "src", "a." + channel, CONSUMER, OPTIONS));
  }

  @Test
  void combinedLength_250_rejected() {
    String channel = "c".repeat(248);
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("a", channel, "src", "a." + channel, CONSUMER, OPTIONS));
  }

  @Test
  void nullSourceId_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration(null, "orders", "src", "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void blankSourceId_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("  ", "orders", "src", "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void nullChannel_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", null, "src", "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void blankChannel_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "  ", "src", "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void nullSourceTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", null, "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void blankSourceTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", "  ", "shopify.orders", CONSUMER, OPTIONS));
  }

  @Test
  void nullTargetTopic_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", "src", null, CONSUMER, OPTIONS));
  }

  @Test
  void nullConsumer_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", "src", "shopify.orders", null, OPTIONS));
  }

  @Test
  void nullOptions_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", "src", "shopify.orders", CONSUMER, null));
  }

  @Test
  void targetTopicMismatch_rejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Registration("shopify", "orders", "src", "wrong.topic", CONSUMER, OPTIONS));
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
}
