package com.unifieddataprocessing.pubsub.kinesis;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration for the Amazon Kinesis-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubConsumer}.
 */
public final class KinesisConsumerConfig {

  /** Kinesis's documented {@code GetRecords} {@code Limit} cap, also the default. */
  public static final int DEFAULT_GET_RECORDS_LIMIT = 10_000;

  /**
   * Default minimum interval between {@code GetRecords} calls per shard — 200 ms = 5 TPS, the
   * Kinesis-enforced ceiling. Set to {@link Duration#ZERO} to disable throttling.
   */
  public static final Duration DEFAULT_GET_RECORDS_MIN_INTERVAL = Duration.ofMillis(200);

  private final String streamName;
  private final Region region;
  private final AwsCredentialsProvider credentialsProvider;
  private final KinesisStartingPosition startingPosition;
  private final int getRecordsLimit;
  private final Duration getRecordsMinInterval;
  private final Map<String, KinesisStartingPosition> startingPositionByShard;

  /** Creates a config with {@link KinesisStartingPosition#latest()} and all defaults. */
  public KinesisConsumerConfig(
      String streamName, Region region, AwsCredentialsProvider credentialsProvider) {
    this(
        streamName,
        region,
        credentialsProvider,
        KinesisStartingPosition.latest(),
        DEFAULT_GET_RECORDS_LIMIT,
        DEFAULT_GET_RECORDS_MIN_INTERVAL,
        Collections.emptyMap());
  }

  /** Convenience constructor using {@link #DEFAULT_GET_RECORDS_MIN_INTERVAL}. */
  public KinesisConsumerConfig(
      String streamName,
      Region region,
      AwsCredentialsProvider credentialsProvider,
      KinesisStartingPosition startingPosition,
      int getRecordsLimit) {
    this(
        streamName,
        region,
        credentialsProvider,
        startingPosition,
        getRecordsLimit,
        DEFAULT_GET_RECORDS_MIN_INTERVAL,
        Collections.emptyMap());
  }

  /** Convenience constructor with no per-shard starting-position overrides. */
  public KinesisConsumerConfig(
      String streamName,
      Region region,
      AwsCredentialsProvider credentialsProvider,
      KinesisStartingPosition startingPosition,
      int getRecordsLimit,
      Duration getRecordsMinInterval) {
    this(
        streamName,
        region,
        credentialsProvider,
        startingPosition,
        getRecordsLimit,
        getRecordsMinInterval,
        Collections.emptyMap());
  }

  /**
   * Full configuration. {@code getRecordsLimit} must be in {@code (0, 10000]} (Kinesis's
   * GetRecords cap). {@code getRecordsMinInterval} must be non-null and non-negative;
   * {@link Duration#ZERO} disables per-shard throttling.
   *
   * <p>{@code startingPositionByShard} lets multi-shard streams resume each shard from its own
   * checkpoint (round-tripped via {@link KinesisConsumer#getCheckpoints()}). Shards not present
   * in the map fall back to {@code startingPosition}; new shards introduced by resharding after
   * the checkpoints were taken therefore start from the configured global default. May be
   * {@code null} (treated as empty).
   */
  public KinesisConsumerConfig(
      String streamName,
      Region region,
      AwsCredentialsProvider credentialsProvider,
      KinesisStartingPosition startingPosition,
      int getRecordsLimit,
      Duration getRecordsMinInterval,
      Map<String, KinesisStartingPosition> startingPositionByShard) {
    this.streamName = Objects.requireNonNull(streamName, "streamName");
    this.region = Objects.requireNonNull(region, "region");
    this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
    this.startingPosition = Objects.requireNonNull(startingPosition, "startingPosition");
    if (getRecordsLimit <= 0 || getRecordsLimit > DEFAULT_GET_RECORDS_LIMIT) {
      throw new IllegalArgumentException(
          "getRecordsLimit must be in (0, " + DEFAULT_GET_RECORDS_LIMIT + "]");
    }
    this.getRecordsLimit = getRecordsLimit;
    Objects.requireNonNull(getRecordsMinInterval, "getRecordsMinInterval");
    if (getRecordsMinInterval.isNegative()) {
      throw new IllegalArgumentException("getRecordsMinInterval must not be negative");
    }
    this.getRecordsMinInterval = getRecordsMinInterval;
    this.startingPositionByShard =
        startingPositionByShard == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(startingPositionByShard));
  }

  public String getStreamName() {
    return streamName;
  }

  public Region getRegion() {
    return region;
  }

  public AwsCredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public KinesisStartingPosition getStartingPosition() {
    return startingPosition;
  }

  public int getGetRecordsLimit() {
    return getRecordsLimit;
  }

  public Duration getGetRecordsMinInterval() {
    return getRecordsMinInterval;
  }

  public Map<String, KinesisStartingPosition> getStartingPositionByShard() {
    return startingPositionByShard;
  }
}
