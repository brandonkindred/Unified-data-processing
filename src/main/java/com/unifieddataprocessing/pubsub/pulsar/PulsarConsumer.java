package com.unifieddataprocessing.pubsub.pulsar;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * Apache Pulsar-backed {@link PubSubConsumer}. Uses per-message acknowledgement via Pulsar's native
 * {@link MessageId} (no offset/watermark logic).
 *
 * <p>Multi-topic subscriptions are modelled by holding one inner Pulsar {@link Consumer} per
 * topic. The Pulsar Java client cannot add or remove topics on an existing {@code Consumer}, so
 * each {@link #subscribe(String)} that names a new topic builds a fresh inner consumer and each
 * {@link #unsubscribe(String)} closes only that one — leaving in-flight ack state on the other
 * topics intact, which is what our "ack failure leaves state for retry" contract requires.
 *
 * <p>Pattern-based subscriptions ({@code topicsPattern}) are intentionally out of scope; the
 * framework {@code subscribe(String)} verb takes a literal topic name.
 *
 * <p>Framework {@code Message.id} format is {@code "<topic>-<MessageId.toString()>"} so that
 * identical Pulsar positions on different topics do not collide in the per-message ack side-map.
 *
 * <p>Not thread-safe.
 */
public class PulsarConsumer implements PubSubConsumer {

  /** Seam for the underlying Pulsar client and per-topic consumer construction. */
  interface Factory {
    PulsarClient newClient(PulsarConsumerConfig config) throws PulsarClientException;

    Consumer<byte[]> newConsumer(
        PulsarClient client, PulsarConsumerConfig config, Collection<String> topics)
        throws PulsarClientException;
  }

  private final PulsarConsumerConfig config;
  private final Factory factory;
  private final Map<String, Consumer<byte[]>> consumersByTopic = new LinkedHashMap<>();
  // Side-map: framework Message.id -> (Pulsar MessageId, owning inner Consumer).
  // Owner reference lets ack route to the right per-topic consumer without
  // re-deriving topic from the id.
  private final Map<String, AckEntry> ackEntries = new HashMap<>();

  private PulsarClient client;

  /** Creates a consumer that builds real Pulsar clients on {@link #connect()}. */
  public PulsarConsumer(PulsarConsumerConfig config) {
    this(
        config,
        new Factory() {
          @Override
          public PulsarClient newClient(PulsarConsumerConfig c) throws PulsarClientException {
            return c.applyToClientBuilder(PulsarClient.builder()).build();
          }

          @Override
          public Consumer<byte[]> newConsumer(
              PulsarClient client, PulsarConsumerConfig c, Collection<String> topics)
              throws PulsarClientException {
            return c.applyToConsumerBuilder(client.newConsumer(Schema.BYTES), topics).subscribe();
          }
        });
  }

  PulsarConsumer(PulsarConsumerConfig config, Factory factory) {
    this.config = Objects.requireNonNull(config, "config");
    this.factory = Objects.requireNonNull(factory, "factory");
  }

  @Override
  public void connect() {
    if (client != null) {
      throw new IllegalStateException("already connected");
    }
    try {
      client = factory.newClient(config);
    } catch (PulsarClientException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void subscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    if (consumersByTopic.containsKey(topic)) {
      return;
    }
    try {
      Consumer<byte[]> inner = factory.newConsumer(client, config, List.of(topic));
      consumersByTopic.put(topic, inner);
    } catch (PulsarClientException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void unsubscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    Consumer<byte[]> inner = consumersByTopic.get(topic);
    if (inner == null) {
      return;
    }
    // Remove only after a successful close so a transient close failure stays retryable —
    // otherwise we'd lose the only reference to a still-open inner consumer and leak the
    // underlying Pulsar subscription. Pulsar's Consumer.close() is documented idempotent,
    // so a successful retry after a partial close is safe.
    try {
      inner.close();
    } catch (PulsarClientException e) {
      throw new UncheckedIOException(e);
    }
    consumersByTopic.remove(topic);
  }

  @Override
  public List<Message> poll(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    ensureConnected();
    if (consumersByTopic.isEmpty()) {
      return Collections.emptyList();
    }

    // Single absolute deadline keeps poll() bounded by `timeout` regardless of how many
    // inner consumers we visit or how messages arrive. Each receive() call still caps at
    // perConsumerMs to give other consumers a fair shot when no traffic is available.
    long deadlineNs = System.nanoTime() + timeout.toNanos();
    int n = consumersByTopic.size();
    long perConsumerMs = Math.max(1L, timeout.toMillis() / n);
    int budget = config.getMaxMessagesPerPoll();
    List<Message> out = new ArrayList<>();

    for (Map.Entry<String, Consumer<byte[]>> entry : consumersByTopic.entrySet()) {
      if (out.size() >= budget || System.nanoTime() >= deadlineNs) {
        break;
      }
      String topic = entry.getKey();
      Consumer<byte[]> inner = entry.getValue();
      while (out.size() < budget) {
        long remainingMs = (deadlineNs - System.nanoTime()) / 1_000_000L;
        if (remainingMs <= 0) {
          break;
        }
        int receiveMs = (int) Math.min(perConsumerMs, remainingMs);
        org.apache.pulsar.client.api.Message<byte[]> msg;
        try {
          msg = inner.receive(receiveMs, TimeUnit.MILLISECONDS);
        } catch (PulsarClientException e) {
          throw new UncheckedIOException(e);
        }
        if (msg == null) {
          break;
        }
        out.add(toFrameworkMessage(msg, topic, inner));
      }
    }
    return out;
  }

  private Message toFrameworkMessage(
      org.apache.pulsar.client.api.Message<byte[]> pm, String topic, Consumer<byte[]> owner) {
    // Pulsar MessageId is a position within a topic/partition (ledger:entry:partition:batch)
    // and does NOT encode the topic, so identical positions on different topics collide in a
    // single-keyed side-map. Prefix with topic to make the framework id unique across topics
    // (mirrors KafkaConsumer's "topic-partition-offset" id format).
    String id = topic + "-" + pm.getMessageId().toString();
    byte[] payload = pm.getValue() == null ? new byte[0] : pm.getValue();
    Map<String, String> rawProps = pm.getProperties();
    Map<String, String> attributes =
        rawProps == null ? Collections.emptyMap() : new LinkedHashMap<>(rawProps);
    ackEntries.put(id, new AckEntry(pm.getMessageId(), owner));
    return new Message(id, topic, payload, attributes);
  }

  @Override
  public void acknowledge(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    AckEntry entry = ackEntries.get(message.getId());
    if (entry == null) {
      throw new IllegalStateException(
          "Unknown message: "
              + message.getId()
              + ". Only messages returned by poll() can be acknowledged.");
    }
    // On RPC failure, leave the side-map entry intact so the caller can retry.
    try {
      entry.owner.acknowledge(entry.pulsarId);
    } catch (PulsarClientException e) {
      throw new UncheckedIOException(e);
    }
    ackEntries.remove(message.getId());
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      for (Consumer<byte[]> inner : consumersByTopic.values()) {
        try {
          inner.close();
        } catch (PulsarClientException ignored) {
          // best-effort: continue closing the rest, then the client
        }
      }
      try {
        client.close();
      } catch (PulsarClientException ignored) {
        // best-effort
      }
    } finally {
      client = null;
      consumersByTopic.clear();
      ackEntries.clear();
    }
  }

  private void ensureConnected() {
    if (client == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }

  private static final class AckEntry {
    final MessageId pulsarId;
    final Consumer<byte[]> owner;

    AckEntry(MessageId pulsarId, Consumer<byte[]> owner) {
      this.pulsarId = pulsarId;
      this.owner = owner;
    }
  }
}
