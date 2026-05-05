package com.unifieddataprocessing.pubsub.gcp;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Google Cloud Pub/Sub-backed {@link PubSubConsumer}. Wraps a low-level {@link SubscriberStub} and
 * uses its synchronous pull RPC so that {@link #poll(Duration)} is a single deadline-bounded call.
 *
 * <p>Bound to the single subscription identified by {@link
 * GcpPubSubConsumerConfig#getSubscriptionId()}: {@link #subscribe(String)} accepts only that name.
 * Multi-subscription routing is intentionally out of scope and would warrant a separate
 * implementation. Per-message ack uses Pub/Sub ack ids — there is no offset/watermark logic.
 *
 * <p>Not thread-safe; the underlying gRPC stub is reusable across threads but the per-instance ack
 * bookkeeping here is not.
 */
public class GcpPubSubConsumer implements PubSubConsumer {

  private final GcpPubSubConsumerConfig config;
  private final Function<SubscriberStubSettings, SubscriberStub> stubFactory;
  private final Set<String> subscribedTopics = new LinkedHashSet<>();
  // Pub/Sub-assigned messageId → latest ackId. Redeliveries reuse the entry
  // because each delivery gets a fresh ackId and only the latest is valid.
  private final Map<String, String> ackIdByMessageId = new HashMap<>();

  private SubscriberStub stub;

  /** Creates a consumer that builds a real {@link GrpcSubscriberStub} on {@link #connect()}. */
  public GcpPubSubConsumer(GcpPubSubConsumerConfig config) {
    this(
        config,
        settings -> {
          try {
            return GrpcSubscriberStub.create(settings);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  GcpPubSubConsumer(
      GcpPubSubConsumerConfig config,
      Function<SubscriberStubSettings, SubscriberStub> stubFactory) {
    this.config = Objects.requireNonNull(config, "config");
    this.stubFactory = Objects.requireNonNull(stubFactory, "stubFactory");
  }

  @Override
  public void connect() {
    if (stub != null) {
      throw new IllegalStateException("already connected");
    }
    stub = stubFactory.apply(config.toSubscriberStubSettings());
  }

  @Override
  public void subscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    if (!topic.equals(config.getSubscriptionId())) {
      throw new IllegalArgumentException(
          "this consumer is bound to subscription '"
              + config.getSubscriptionId()
              + "'; multi-subscription routing is not supported (got '"
              + topic
              + "')");
    }
    subscribedTopics.add(topic);
  }

  @Override
  public void unsubscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    subscribedTopics.remove(topic);
  }

  @Override
  public List<Message> poll(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    ensureConnected();
    if (subscribedTopics.isEmpty()) {
      return Collections.emptyList();
    }
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(config.getProjectSubscriptionName())
            .setMaxMessages(config.getMaxMessagesPerPoll())
            .build();
    PullResponse response =
        stub.pullCallable()
            .call(
                request,
                GrpcCallContext.createDefault()
                    .withTimeout(org.threeten.bp.Duration.ofNanos(timeout.toNanos())));
    List<ReceivedMessage> received = response.getReceivedMessagesList();
    if (received.isEmpty()) {
      return Collections.emptyList();
    }
    String topic = config.getSubscriptionId();
    List<Message> result = new ArrayList<>(received.size());
    for (ReceivedMessage rm : received) {
      String id = rm.getMessage().getMessageId();
      byte[] payload = rm.getMessage().getData().toByteArray();
      Map<String, String> attributes = new LinkedHashMap<>(rm.getMessage().getAttributesMap());
      ackIdByMessageId.put(id, rm.getAckId());
      result.add(new Message(id, topic, payload, attributes));
    }
    return result;
  }

  @Override
  public void acknowledge(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    String ackId = ackIdByMessageId.get(message.getId());
    if (ackId == null) {
      throw new IllegalStateException(
          "Unknown message: "
              + message.getId()
              + ". Only messages returned by poll() can be acknowledged.");
    }
    AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription(config.getProjectSubscriptionName())
            .addAckIds(ackId)
            .build();
    // On RPC failure, leave the side-map entry intact so the caller can retry
    // (mirrors the "commit failure leaves state" semantics of KafkaConsumer).
    stub.acknowledgeCallable().call(request);
    ackIdByMessageId.remove(message.getId());
  }

  @Override
  public void close() {
    if (stub == null) {
      return;
    }
    try {
      stub.close();
    } finally {
      stub = null;
      subscribedTopics.clear();
      ackIdByMessageId.clear();
    }
  }

  private void ensureConnected() {
    if (stub == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }
}
