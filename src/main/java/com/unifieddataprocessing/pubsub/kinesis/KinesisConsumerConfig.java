package com.unifieddataprocessing.pubsub.kinesis;

import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration for the Amazon Kinesis-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubConsumer}.
 */
public final class KinesisConsumerConfig {

  /** Default and SDK-maximum {@code Limit} for {@code GetRecords}. */
  public static final int DEFAULT_GET_RECORDS_LIMIT = 1000;

  private final String streamName;
  private final Region region;
  private final AwsCredentialsProvider credentialsProvider;
  private final KinesisStartingPosition startingPosition;
  private final int getRecordsLimit;

  /** Creates a config with {@link KinesisStartingPosition#latest()} and the default limit. */
  public KinesisConsumerConfig(
      String streamName, Region region, AwsCredentialsProvider credentialsProvider) {
    this(
        streamName,
        region,
        credentialsProvider,
        KinesisStartingPosition.latest(),
        DEFAULT_GET_RECORDS_LIMIT);
  }

  /**
   * Full configuration. {@code getRecordsLimit} must be in {@code (0, 1000]} (Kinesis's
   * GetRecords cap).
   */
  public KinesisConsumerConfig(
      String streamName,
      Region region,
      AwsCredentialsProvider credentialsProvider,
      KinesisStartingPosition startingPosition,
      int getRecordsLimit) {
    this.streamName = Objects.requireNonNull(streamName, "streamName");
    this.region = Objects.requireNonNull(region, "region");
    this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
    this.startingPosition = Objects.requireNonNull(startingPosition, "startingPosition");
    if (getRecordsLimit <= 0 || getRecordsLimit > DEFAULT_GET_RECORDS_LIMIT) {
      throw new IllegalArgumentException(
          "getRecordsLimit must be in (0, " + DEFAULT_GET_RECORDS_LIMIT + "]");
    }
    this.getRecordsLimit = getRecordsLimit;
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
}
