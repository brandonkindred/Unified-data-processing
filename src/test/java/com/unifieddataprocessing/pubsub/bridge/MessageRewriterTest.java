package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageRewriterTest {

  private static final PubSubConsumer CONSUMER = new NoOpConsumer();
  private static final Registration REGISTRATION =
      new Registration(
          "shopify",
          "orders",
          "shopify-orders-sub",
          "shopify.orders",
          CONSUMER,
          ChannelOptions.defaults());

  @Test
  void targetTopic_setToRegistrationTargetTopic() {
    Message src = new Message("id-1", "ignored-source-topic", payload("payload"), Map.of());
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("shopify.orders", out.getTopic());
  }

  @Test
  void payload_preserved() {
    byte[] payload = payload("hello-world");
    Message src = new Message("id-1", "src-topic", payload, Map.of());
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertArrayEquals(payload, out.getPayload());
  }

  @Test
  void id_preserved() {
    Message src = new Message("the-id", "src-topic", payload("p"), Map.of());
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("the-id", out.getId());
  }

  @Test
  void kafkaKeyAttribute_preserved() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(KafkaProducer.ATTR_KEY, "partition-key-42");
    Message src = new Message("id-1", "src-topic", payload("p"), attrs);
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("partition-key-42", out.getAttributes().get(KafkaProducer.ATTR_KEY));
  }

  @Test
  void bridgeSourceIdAttribute_overwritesCallerValue() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_ID, "wrong");
    Message src = new Message("id-1", "src-topic", payload("p"), attrs);
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("shopify", out.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_ID));
  }

  @Test
  void bridgeSourceTopicAttribute_setToRegistrationSourceTopic_notInputTopic() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_TOPIC, "caller-set-value");
    Message src = new Message("id-1", "input-topic-differs", payload("p"), attrs);
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals(
        "shopify-orders-sub", out.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_TOPIC));
  }

  @Test
  void bridgeChannelAttribute_setToRegistrationChannel() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(BridgeAttributes.BRIDGE_CHANNEL, "wrong-channel");
    Message src = new Message("id-1", "src-topic", payload("p"), attrs);
    Message out = MessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("orders", out.getAttributes().get(BridgeAttributes.BRIDGE_CHANNEL));
  }

  private static byte[] payload(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
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
