package com.unifieddataprocessing.pubsub.relay;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishResult;
import com.unifieddataprocessing.pubsub.bridge.BridgeAttributes;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RelayMessageRewriterTest {

  private static final PubSubConsumer CONSUMER = new NoOpConsumer();
  private static final PubSubPublisher PUBLISHER = new NoOpPublisher();
  private static final RelayRegistration REGISTRATION =
      new RelayRegistration("rabbit-prod", "shopify.orders", "orders_q", CONSUMER, PUBLISHER);

  @Test
  void targetTopic_setToDownstreamTopic() {
    Message src = new Message("id-1", "shopify.orders", payload("p"), Map.of());
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("orders_q", out.getTopic());
  }

  @Test
  void payload_preserved() {
    byte[] payload = payload("hello-world");
    Message src = new Message("id-1", "shopify.orders", payload, Map.of());
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertArrayEquals(payload, out.getPayload());
  }

  @Test
  void id_preserved() {
    Message src = new Message("the-id", "shopify.orders", payload("p"), Map.of());
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("the-id", out.getId());
  }

  @Test
  void kafkaKeyAttribute_preserved() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(KafkaProducer.ATTR_KEY, "partition-key-42");
    Message src = new Message("id-1", "shopify.orders", payload("p"), attrs);
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("partition-key-42", out.getAttributes().get(KafkaProducer.ATTR_KEY));
  }

  @Test
  void bridgeProvenanceAttributes_preserved() {
    // Messages coming off the unified backbone carry BridgeAttributes set by the inbound bridge.
    // The relay must preserve them so downstream consumers can recover the full path.
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_ID, "shopify");
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_TOPIC, "shopify-orders-sub");
    attrs.put(BridgeAttributes.BRIDGE_CHANNEL, "orders");
    Message src = new Message("id-1", "shopify.orders", payload("p"), attrs);
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("shopify", out.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_ID));
    assertEquals(
        "shopify-orders-sub", out.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_TOPIC));
    assertEquals("orders", out.getAttributes().get(BridgeAttributes.BRIDGE_CHANNEL));
  }

  @Test
  void relayDestinationIdAttribute_overwritesCallerValue() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(RelayAttributes.RELAY_DESTINATION_ID, "wrong");
    Message src = new Message("id-1", "shopify.orders", payload("p"), attrs);
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("rabbit-prod", out.getAttributes().get(RelayAttributes.RELAY_DESTINATION_ID));
  }

  @Test
  void relaySourceTopicAttribute_setToRegistrationSourceTopic() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(RelayAttributes.RELAY_SOURCE_TOPIC, "caller-set-value");
    Message src = new Message("id-1", "shopify.orders", payload("p"), attrs);
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("shopify.orders", out.getAttributes().get(RelayAttributes.RELAY_SOURCE_TOPIC));
  }

  @Test
  void relayDownstreamTopicAttribute_setToRegistrationDownstreamTopic() {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(RelayAttributes.RELAY_DOWNSTREAM_TOPIC, "wrong-downstream");
    Message src = new Message("id-1", "shopify.orders", payload("p"), attrs);
    Message out = RelayMessageRewriter.rewrite(src, REGISTRATION);
    assertEquals("orders_q", out.getAttributes().get(RelayAttributes.RELAY_DOWNSTREAM_TOPIC));
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
