package com.unifieddataprocessing.pubsub.kinesis;

import java.time.Duration;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration for the Amazon Kinesis-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubPublisher}. No {@code streamName} field — the consumer's
 * config is single-stream-bound but the publisher takes the stream name from each {@link
 * com.unifieddataprocessing.pubsub.Message#getTopic()} and can fan out across many streams.
 */
public final class KinesisPublisherConfig {

  /** Kinesis's documented {@code PutRecords} limit (records per call). */
  public static final int DEFAULT_MAX_RECORDS_PER_BATCH = 500;

  /** Default size of the executor pool that offloads sync {@code putRecord} calls. */
  public static final int DEFAULT_PUBLISH_CONCURRENCY = 4;

  /** Default time {@link com.unifieddataprocessing.pubsub.PubSubPublisher#close()} waits. */
  public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private final Region region;
  private final AwsCredentialsProvider credentialsProvider;
  private final int maxRecordsPerBatch;
  private final int publishConcurrency;
  private final Duration closeTimeout;

  /** Creates a config with all defaults. */
  public KinesisPublisherConfig(Region region, AwsCredentialsProvider credentialsProvider) {
    this(
        region,
        credentialsProvider,
        DEFAULT_MAX_RECORDS_PER_BATCH,
        DEFAULT_PUBLISH_CONCURRENCY,
        DEFAULT_CLOSE_TIMEOUT);
  }

  /**
   * Full configuration. {@code maxRecordsPerBatch} must be in {@code (0, 500]} (Kinesis's
   * PutRecords cap). {@code publishConcurrency} must be {@code > 0}. {@code closeTimeout} must be
   * non-null and non-negative.
   */
  public KinesisPublisherConfig(
      Region region,
      AwsCredentialsProvider credentialsProvider,
      int maxRecordsPerBatch,
      int publishConcurrency,
      Duration closeTimeout) {
    this.region = Objects.requireNonNull(region, "region");
    this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
    if (maxRecordsPerBatch <= 0 || maxRecordsPerBatch > DEFAULT_MAX_RECORDS_PER_BATCH) {
      throw new IllegalArgumentException(
          "maxRecordsPerBatch must be in (0, " + DEFAULT_MAX_RECORDS_PER_BATCH + "]");
    }
    this.maxRecordsPerBatch = maxRecordsPerBatch;
    if (publishConcurrency <= 0) {
      throw new IllegalArgumentException("publishConcurrency must be > 0");
    }
    this.publishConcurrency = publishConcurrency;
    Objects.requireNonNull(closeTimeout, "closeTimeout");
    if (closeTimeout.isNegative()) {
      throw new IllegalArgumentException("closeTimeout must not be negative");
    }
    this.closeTimeout = closeTimeout;
  }

  public Region getRegion() {
    return region;
  }

  public AwsCredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public int getMaxRecordsPerBatch() {
    return maxRecordsPerBatch;
  }

  public int getPublishConcurrency() {
    return publishConcurrency;
  }

  public Duration getCloseTimeout() {
    return closeTimeout;
  }
}
