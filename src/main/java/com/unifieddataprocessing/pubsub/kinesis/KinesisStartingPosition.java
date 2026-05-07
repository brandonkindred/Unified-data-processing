package com.unifieddataprocessing.pubsub.kinesis;

import java.time.Instant;
import java.util.Objects;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * Where to start reading from a Kinesis shard. Use the static factory methods to construct one
 * matching the desired {@link ShardIteratorType}.
 */
public final class KinesisStartingPosition {

  private final ShardIteratorType type;
  private final Instant timestamp;
  private final String sequenceNumber;

  private KinesisStartingPosition(
      ShardIteratorType type, Instant timestamp, String sequenceNumber) {
    this.type = type;
    this.timestamp = timestamp;
    this.sequenceNumber = sequenceNumber;
  }

  /** Start at the most recent record after the consumer subscribes. */
  public static KinesisStartingPosition latest() {
    return new KinesisStartingPosition(ShardIteratorType.LATEST, null, null);
  }

  /** Start at the oldest record still retained in the shard. */
  public static KinesisStartingPosition trimHorizon() {
    return new KinesisStartingPosition(ShardIteratorType.TRIM_HORIZON, null, null);
  }

  /** Start reading at the given timestamp. */
  public static KinesisStartingPosition atTimestamp(Instant timestamp) {
    return new KinesisStartingPosition(
        ShardIteratorType.AT_TIMESTAMP, Objects.requireNonNull(timestamp, "timestamp"), null);
  }

  /** Start reading at the given sequence number (inclusive). */
  public static KinesisStartingPosition atSequenceNumber(String sequenceNumber) {
    return new KinesisStartingPosition(
        ShardIteratorType.AT_SEQUENCE_NUMBER,
        null,
        Objects.requireNonNull(sequenceNumber, "sequenceNumber"));
  }

  /** Start reading after the given sequence number (exclusive). */
  public static KinesisStartingPosition afterSequenceNumber(String sequenceNumber) {
    return new KinesisStartingPosition(
        ShardIteratorType.AFTER_SEQUENCE_NUMBER,
        null,
        Objects.requireNonNull(sequenceNumber, "sequenceNumber"));
  }

  public ShardIteratorType getType() {
    return type;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getSequenceNumber() {
    return sequenceNumber;
  }
}
