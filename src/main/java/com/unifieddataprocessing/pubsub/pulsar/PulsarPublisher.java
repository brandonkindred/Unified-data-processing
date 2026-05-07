package com.unifieddataprocessing.pubsub.pulsar;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Apache Pulsar-backed {@link PubSubPublisher}. Mirrors the multi-topic structure of {@link
 * PulsarConsumer}: holds one inner Pulsar {@link Producer} per topic in a {@link LinkedHashMap}
 * cache, lazy-creating on first publish to that topic.
 *
 * <p>Not thread-safe; the inner Pulsar producers are thread-safe but the per-instance cache
 * bookkeeping in this wrapper is not.
 */
public class PulsarPublisher implements PubSubPublisher {

  /** Seam for Pulsar client and per-topic producer construction. */
  interface Factory {
    PulsarClient newClient(PulsarPublisherConfig config) throws PulsarClientException;

    Producer<byte[]> newProducer(PulsarClient client, PulsarPublisherConfig config, String topic)
        throws PulsarClientException;
  }

  private final PulsarPublisherConfig config;
  private final Factory factory;
  private final Map<String, Producer<byte[]>> producersByTopic = new LinkedHashMap<>();

  private PulsarClient client;

  /** Creates a publisher that builds real Pulsar clients and producers on demand. */
  public PulsarPublisher(PulsarPublisherConfig config) {
    this(
        config,
        new Factory() {
          @Override
          public PulsarClient newClient(PulsarPublisherConfig c) throws PulsarClientException {
            return c.applyToClientBuilder(PulsarClient.builder()).build();
          }

          @Override
          public Producer<byte[]> newProducer(
              PulsarClient client, PulsarPublisherConfig c, String topic)
              throws PulsarClientException {
            return c.applyToProducerBuilder(client.newProducer(Schema.BYTES), topic).create();
          }
        });
  }

  PulsarPublisher(PulsarPublisherConfig config, Factory factory) {
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
  public CompletableFuture<PublishResult> publish(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    return doPublish(message);
  }

  @Override
  public PublishResult publishSync(Message message) {
    try {
      return publish(message).join();
    } catch (CompletionException e) {
      throw unwrap(e);
    }
  }

  @Override
  public CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages) {
    Objects.requireNonNull(messages, "messages");
    ensureConnected();
    if (messages.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    List<CompletableFuture<PublishResult>> perMessage = new ArrayList<>(messages.size());
    for (Message m : messages) {
      Objects.requireNonNull(m, "messages contains null");
      perMessage.add(doPublish(m));
    }
    return aggregate(perMessage);
  }

  @Override
  public void flush() {
    ensureConnected();
    if (producersByTopic.isEmpty()) {
      return;
    }
    CompletableFuture<?>[] flushes =
        producersByTopic.values().stream()
            .map(Producer::flushAsync)
            .toArray(CompletableFuture[]::new);
    try {
      CompletableFuture.allOf(flushes).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof PulsarClientException pce) {
        throw new UncheckedIOException(pce);
      }
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      throw e;
    }
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      for (Producer<byte[]> producer : producersByTopic.values()) {
        try {
          producer.close();
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
      producersByTopic.clear();
    }
  }

  private CompletableFuture<PublishResult> doPublish(Message message) {
    String topic = message.getTopic();
    Producer<byte[]> producer = producersByTopic.get(topic);
    if (producer == null) {
      try {
        producer = factory.newProducer(client, config, topic);
      } catch (PulsarClientException e) {
        CompletableFuture<PublishResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(new UncheckedIOException(e));
        return failed;
      }
      producersByTopic.put(topic, producer);
    }
    Producer<byte[]> p = producer;
    TypedMessageBuilder<byte[]> builder = producer.newMessage(Schema.BYTES);
    builder.value(message.getPayload());
    if (!message.getAttributes().isEmpty()) {
      builder.properties(message.getAttributes());
    }
    return builder
        .sendAsync()
        .thenApply(
            (MessageId mid) ->
                PublishResult.forPulsar(
                    topic, topic + "-" + mid.toString(), String.valueOf(p.getLastSequenceId())));
  }

  private static CompletableFuture<List<PublishResult>> aggregate(
      List<CompletableFuture<PublishResult>> perMessage) {
    CompletableFuture<Void> all =
        CompletableFuture.allOf(perMessage.toArray(new CompletableFuture<?>[0]));
    return all.handle(
        (v, t) -> {
          List<PublishResult> succeeded = new ArrayList<>();
          Map<Integer, Throwable> failures = new LinkedHashMap<>();
          for (int i = 0; i < perMessage.size(); i++) {
            CompletableFuture<PublishResult> f = perMessage.get(i);
            if (f.isCompletedExceptionally()) {
              try {
                f.join();
              } catch (CompletionException ce) {
                failures.put(i, ce.getCause() == null ? ce : ce.getCause());
              } catch (RuntimeException re) {
                failures.put(i, re);
              }
            } else {
              succeeded.add(f.join());
            }
          }
          if (failures.isEmpty()) {
            return succeeded;
          }
          throw new PublishBatchException(succeeded, failures);
        });
  }

  private static RuntimeException unwrap(CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException re) {
      return re;
    }
    if (cause == null) {
      return e;
    }
    return new RuntimeException(cause);
  }

  private void ensureConnected() {
    if (client == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }
}
