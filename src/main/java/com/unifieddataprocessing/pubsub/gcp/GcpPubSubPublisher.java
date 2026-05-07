package com.unifieddataprocessing.pubsub.gcp;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Google Cloud Pub/Sub-backed {@link PubSubPublisher}. Wraps the high-level {@link Publisher}
 * client (rather than the gRPC stub the consumer uses) because it natively returns {@link
 * ApiFuture} and handles batching via {@link com.google.api.gax.batching.BatchingSettings}.
 *
 * <p>{@link Publisher} is bound to a single topic, so this wrapper keeps a per-topic cache and
 * lazy-creates entries on first publish to that topic. {@link #connect()} therefore does not
 * eagerly build any client — it only flips a "connected" flag.
 *
 * <p>Note: the GCP consumer takes a <em>subscriptionId</em> in its config, while this publisher
 * takes only a <em>projectId</em> and resolves topics per-message.
 *
 * <p>Not thread-safe.
 */
public class GcpPubSubPublisher implements PubSubPublisher {

  private final GcpPubSubPublisherConfig config;
  private final Function<TopicName, Publisher> publisherFactory;
  private final Map<String, Publisher> publishersByTopic = new LinkedHashMap<>();
  private final Set<CompletableFuture<PublishResult>> inflight = new LinkedHashSet<>();

  private boolean connected;

  /** Creates a publisher that builds real per-topic {@link Publisher}s on first publish. */
  public GcpPubSubPublisher(GcpPubSubPublisherConfig config) {
    this(
        config,
        topic -> {
          try {
            return config.applyToBuilder(topic).build();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  GcpPubSubPublisher(
      GcpPubSubPublisherConfig config, Function<TopicName, Publisher> publisherFactory) {
    this.config = Objects.requireNonNull(config, "config");
    this.publisherFactory = Objects.requireNonNull(publisherFactory, "publisherFactory");
  }

  @Override
  public void connect() {
    if (connected) {
      throw new IllegalStateException("already connected");
    }
    connected = true;
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
    for (Publisher p : publishersByTopic.values()) {
      p.publishAllOutstanding();
    }
    // publishAllOutstanding only triggers batch dispatch; it does NOT wait for acks. Drain
    // every still-pending wrapper future so callers observe broker acknowledgment on return.
    List<CompletableFuture<PublishResult>> snapshot = new ArrayList<>(inflight);
    if (!snapshot.isEmpty()) {
      try {
        CompletableFuture.allOf(snapshot.toArray(new CompletableFuture<?>[0])).join();
      } catch (CompletionException ignored) {
        // Per-message failures already surface through their own futures; flush() does not
        // re-raise them.
      }
    }
  }

  @Override
  public void close() {
    if (!connected) {
      return;
    }
    try {
      for (Publisher p : publishersByTopic.values()) {
        try {
          p.shutdown();
          p.awaitTermination(config.getCloseTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (RuntimeException ignored) {
          // best-effort: continue closing the rest
        }
      }
    } finally {
      connected = false;
      publishersByTopic.clear();
      inflight.clear();
    }
  }

  private CompletableFuture<PublishResult> doPublish(Message message) {
    String topic = message.getTopic();
    Publisher publisher = publishersByTopic.get(topic);
    if (publisher == null) {
      publisher = publisherFactory.apply(TopicName.of(config.getProjectId(), topic));
      publishersByTopic.put(topic, publisher);
    }
    PubsubMessage pubsub =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(message.getPayload()))
            .putAllAttributes(message.getAttributes())
            .build();
    ApiFuture<String> apiFuture = publisher.publish(pubsub);
    CompletableFuture<PublishResult> cf = new CompletableFuture<>();
    inflight.add(cf);
    cf.whenComplete((r, t) -> inflight.remove(cf));
    ApiFutures.addCallback(
        apiFuture,
        new ApiFutureCallback<String>() {
          @Override
          public void onSuccess(String messageId) {
            cf.complete(PublishResult.forGcp(topic, messageId));
          }

          @Override
          public void onFailure(Throwable t) {
            cf.completeExceptionally(t);
          }
        },
        MoreExecutors.directExecutor());
    return cf;
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
    if (!connected) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }
}
