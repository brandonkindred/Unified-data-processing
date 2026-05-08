package com.unifieddataprocessing.pubsub.gcp;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the Google Cloud Pub/Sub-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubPublisher}. Note: the consumer's config takes a {@code
 * subscriptionId}; the publisher takes only a {@code projectId} and reads each topic from {@link
 * com.unifieddataprocessing.pubsub.Message#getTopic()} so one publisher can fan out across many
 * topics under the same project.
 */
public final class GcpPubSubPublisherConfig {

  /** Default time {@link com.unifieddataprocessing.pubsub.PubSubPublisher#close()} waits. */
  public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private final String projectId;
  private final CredentialsProvider credentialsProvider;
  private final TransportChannelProvider channelProvider;
  private final BatchingSettings batchingSettings;
  private final RetrySettings retrySettings;
  private final Duration closeTimeout;

  public GcpPubSubPublisherConfig(String projectId) {
    this(projectId, null, null, null, null, DEFAULT_CLOSE_TIMEOUT);
  }

  /**
   * Full configuration. {@code credentialsProvider}, {@code channelProvider}, {@code
   * batchingSettings} and {@code retrySettings} may all be {@code null} (Pub/Sub client defaults
   * apply). {@code closeTimeout} must be non-null and non-negative.
   */
  public GcpPubSubPublisherConfig(
      String projectId,
      CredentialsProvider credentialsProvider,
      TransportChannelProvider channelProvider,
      BatchingSettings batchingSettings,
      RetrySettings retrySettings,
      Duration closeTimeout) {
    this.projectId = Objects.requireNonNull(projectId, "projectId");
    this.credentialsProvider = credentialsProvider;
    this.channelProvider = channelProvider;
    this.batchingSettings = batchingSettings;
    this.retrySettings = retrySettings;
    Objects.requireNonNull(closeTimeout, "closeTimeout");
    if (closeTimeout.isNegative()) {
      throw new IllegalArgumentException("closeTimeout must not be negative");
    }
    this.closeTimeout = closeTimeout;
  }

  public String getProjectId() {
    return projectId;
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public TransportChannelProvider getChannelProvider() {
    return channelProvider;
  }

  public BatchingSettings getBatchingSettings() {
    return batchingSettings;
  }

  public RetrySettings getRetrySettings() {
    return retrySettings;
  }

  public Duration getCloseTimeout() {
    return closeTimeout;
  }

  /**
   * Returns a {@link Publisher.Builder} for {@code topic} under this config's project, with any
   * non-null providers / settings applied. The caller is expected to call {@code build()} (which
   * may throw {@link java.io.IOException}).
   */
  public Publisher.Builder applyToBuilder(TopicName topic) {
    Objects.requireNonNull(topic, "topic");
    Publisher.Builder builder = Publisher.newBuilder(topic);
    if (credentialsProvider != null) {
      builder.setCredentialsProvider(credentialsProvider);
    }
    if (channelProvider != null) {
      builder.setChannelProvider(channelProvider);
    }
    if (batchingSettings != null) {
      builder.setBatchingSettings(batchingSettings);
    }
    if (retrySettings != null) {
      builder.setRetrySettings(retrySettings);
    }
    return builder;
  }
}
