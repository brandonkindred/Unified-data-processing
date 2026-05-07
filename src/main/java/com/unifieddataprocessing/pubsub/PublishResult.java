package com.unifieddataprocessing.pubsub;

import java.util.Objects;

/**
 * Outcome of a single successful {@link PubSubPublisher#publish(Message)} call. All
 * backend-specific fields are nullable; only {@link #getMessageId()} and {@link #getTopic()} are
 * always populated.
 *
 * <p>Single-class shape (rather than per-backend subclasses) so callers can program against the
 * framework abstraction without {@code instanceof} switches. Use the static {@code forXxx}
 * factories to construct.
 */
public final class PublishResult {

  private final String messageId;
  private final String topic;
  private final Integer partition;
  private final Long offset;
  private final String shardId;
  private final String sequenceNumber;
  private final Long timestamp;

  private PublishResult(
      String messageId,
      String topic,
      Integer partition,
      Long offset,
      String shardId,
      String sequenceNumber,
      Long timestamp) {
    this.messageId = Objects.requireNonNull(messageId, "messageId");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.partition = partition;
    this.offset = offset;
    this.shardId = shardId;
    this.sequenceNumber = sequenceNumber;
    this.timestamp = timestamp;
  }

  /** Result for a Kafka {@code send} that completed with broker metadata. */
  public static PublishResult forKafka(
      String topic, String messageId, int partition, long offset, long timestamp) {
    return new PublishResult(messageId, topic, partition, offset, null, null, timestamp);
  }

  /** Result for a Google Cloud Pub/Sub publish; {@code messageId} is broker-assigned. */
  public static PublishResult forGcp(String topic, String messageId) {
    return new PublishResult(messageId, topic, null, null, null, null, null);
  }

  /** Result for a Kinesis {@code PutRecord} / {@code PutRecords} entry. */
  public static PublishResult forKinesis(
      String topic, String messageId, String shardId, String sequenceNumber) {
    return new PublishResult(messageId, topic, null, null, shardId, sequenceNumber, null);
  }

  /**
   * Result for a Pulsar {@code sendAsync}; {@code messageId} encodes the broker-assigned position
   * (ledger:entry:partition:batch). The producer-side sequence id is intentionally not surfaced
   * here — it is producer-scoped and racy to read from the async callback under in-flight sends.
   */
  public static PublishResult forPulsar(String topic, String messageId) {
    return new PublishResult(messageId, topic, null, null, null, null, null);
  }

  public String getMessageId() {
    return messageId;
  }

  public String getTopic() {
    return topic;
  }

  /** Kafka partition; {@code null} for other backends. */
  public Integer getPartition() {
    return partition;
  }

  /** Kafka log offset; {@code null} for other backends. */
  public Long getOffset() {
    return offset;
  }

  /** Kinesis shard id; {@code null} for other backends. */
  public String getShardId() {
    return shardId;
  }

  /** Kinesis sequence number; {@code null} otherwise. */
  public String getSequenceNumber() {
    return sequenceNumber;
  }

  /** Kafka record timestamp (epoch ms); {@code null} for other backends. */
  public Long getTimestamp() {
    return timestamp;
  }
}
