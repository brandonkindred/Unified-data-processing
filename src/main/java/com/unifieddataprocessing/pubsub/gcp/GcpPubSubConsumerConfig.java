package com.unifieddataprocessing.pubsub.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Configuration for the Google Cloud Pub/Sub-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubConsumer}.
 */
public final class GcpPubSubConsumerConfig {

  /** Default {@code maxMessages} requested per pull RPC. */
  public static final int DEFAULT_MAX_MESSAGES_PER_POLL = 100;

  private final String projectId;
  private final String subscriptionId;
  private final int maxMessagesPerPoll;
  private final CredentialsProvider credentialsProvider;
  private final TransportChannelProvider channelProvider;

  public GcpPubSubConsumerConfig(String projectId, String subscriptionId) {
    this(projectId, subscriptionId, DEFAULT_MAX_MESSAGES_PER_POLL, null, null);
  }

  /**
   * Full configuration. {@code credentialsProvider} and {@code channelProvider} may be {@code
   * null}, in which case Pub/Sub client defaults apply (Application Default Credentials and the
   * standard gRPC transport).
   */
  public GcpPubSubConsumerConfig(
      String projectId,
      String subscriptionId,
      int maxMessagesPerPoll,
      CredentialsProvider credentialsProvider,
      TransportChannelProvider channelProvider) {
    this.projectId = Objects.requireNonNull(projectId, "projectId");
    this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
    if (maxMessagesPerPoll <= 0) {
      throw new IllegalArgumentException("maxMessagesPerPoll must be > 0");
    }
    this.maxMessagesPerPoll = maxMessagesPerPoll;
    this.credentialsProvider = credentialsProvider;
    this.channelProvider = channelProvider;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public int getMaxMessagesPerPoll() {
    return maxMessagesPerPoll;
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public TransportChannelProvider getChannelProvider() {
    return channelProvider;
  }

  /** Fully-qualified Pub/Sub subscription resource name. */
  public String getProjectSubscriptionName() {
    return "projects/" + projectId + "/subscriptions/" + subscriptionId;
  }

  /** Builds {@link SubscriberStubSettings} applying any non-null providers. */
  public SubscriberStubSettings toSubscriberStubSettings() {
    try {
      SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
      if (credentialsProvider != null) {
        builder.setCredentialsProvider(credentialsProvider);
      }
      if (channelProvider != null) {
        builder.setTransportChannelProvider(channelProvider);
      }
      return builder.build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
