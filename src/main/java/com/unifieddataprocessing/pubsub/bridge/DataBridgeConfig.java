package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable top-level configuration for the data bridge.
 *
 * <p>Wraps a {@link KafkaProducerConfig} plus the timing knobs that govern poll/publish behavior
 * and the topic-provisioning defaults. Build instances via {@link #builder()}; every duration must
 * be strictly positive, and {@code defaultPartitions} / {@code defaultReplicationFactor} must be
 * greater than zero. {@code producerConfig} is required and has no default.
 */
public final class DataBridgeConfig {

  private final KafkaProducerConfig producerConfig;
  private final Duration pollTimeout;
  private final Duration publishTimeout;
  private final Duration shutdownTimeout;
  private final Duration closeForceTimeout;
  private final Duration pollBackoff;
  private final int defaultPartitions;
  private final short defaultReplicationFactor;

  private DataBridgeConfig(Builder b) {
    this.producerConfig = b.producerConfig;
    this.pollTimeout = b.pollTimeout;
    this.publishTimeout = b.publishTimeout;
    this.shutdownTimeout = b.shutdownTimeout;
    this.closeForceTimeout = b.closeForceTimeout;
    this.pollBackoff = b.pollBackoff;
    this.defaultPartitions = b.defaultPartitions;
    this.defaultReplicationFactor = b.defaultReplicationFactor;
  }

  public static Builder builder() {
    return new Builder();
  }

  public KafkaProducerConfig producerConfig() {
    return producerConfig;
  }

  public Duration pollTimeout() {
    return pollTimeout;
  }

  public Duration publishTimeout() {
    return publishTimeout;
  }

  public Duration shutdownTimeout() {
    return shutdownTimeout;
  }

  public Duration closeForceTimeout() {
    return closeForceTimeout;
  }

  public Duration pollBackoff() {
    return pollBackoff;
  }

  public int defaultPartitions() {
    return defaultPartitions;
  }

  public short defaultReplicationFactor() {
    return defaultReplicationFactor;
  }

  /** Fluent builder for {@link DataBridgeConfig}. */
  public static final class Builder {

    private KafkaProducerConfig producerConfig;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration publishTimeout = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration closeForceTimeout = Duration.ofSeconds(5);
    private Duration pollBackoff = Duration.ofSeconds(1);
    private int defaultPartitions = 1;
    private short defaultReplicationFactor = 1;

    private Builder() {}

    /** Sets the Kafka producer config used by the bridge's publisher. Required; non-null. */
    public Builder producerConfig(KafkaProducerConfig producerConfig) {
      this.producerConfig = Objects.requireNonNull(producerConfig, "producerConfig");
      return this;
    }

    /** Poll timeout passed to each registered source consumer. Must be strictly positive. */
    public Builder pollTimeout(Duration pollTimeout) {
      this.pollTimeout = requirePositive("pollTimeout", pollTimeout);
      return this;
    }

    /** Per-message publish timeout for the Kafka publish future. Must be strictly positive. */
    public Builder publishTimeout(Duration publishTimeout) {
      this.publishTimeout = requirePositive("publishTimeout", publishTimeout);
      return this;
    }

    /** Graceful executor termination budget on {@code close()}. Must be strictly positive. */
    public Builder shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = requirePositive("shutdownTimeout", shutdownTimeout);
      return this;
    }

    /**
     * Extra wait after {@code shutdownNow()} for poll threads to fully stop touching the
     * publisher/consumers. Must be strictly positive.
     */
    public Builder closeForceTimeout(Duration closeForceTimeout) {
      this.closeForceTimeout = requirePositive("closeForceTimeout", closeForceTimeout);
      return this;
    }

    /** Backoff applied after a {@code poll()} failure before the next attempt. Must be > 0. */
    public Builder pollBackoff(Duration pollBackoff) {
      this.pollBackoff = requirePositive("pollBackoff", pollBackoff);
      return this;
    }

    /** Default partition count for auto-provisioned topics. Must be > 0. */
    public Builder defaultPartitions(int defaultPartitions) {
      if (defaultPartitions <= 0) {
        throw new IllegalArgumentException(
            "defaultPartitions must be > 0, got " + defaultPartitions);
      }
      this.defaultPartitions = defaultPartitions;
      return this;
    }

    /** Default replication factor for auto-provisioned topics. Must be > 0. */
    public Builder defaultReplicationFactor(short defaultReplicationFactor) {
      if (defaultReplicationFactor <= 0) {
        throw new IllegalArgumentException(
            "defaultReplicationFactor must be > 0, got " + defaultReplicationFactor);
      }
      this.defaultReplicationFactor = defaultReplicationFactor;
      return this;
    }

    /** Builds an immutable {@link DataBridgeConfig}; throws if {@code producerConfig} is unset. */
    public DataBridgeConfig build() {
      if (producerConfig == null) {
        throw new IllegalStateException("producerConfig is required");
      }
      return new DataBridgeConfig(this);
    }

    private static Duration requirePositive(String name, Duration d) {
      Objects.requireNonNull(d, name);
      if (d.isZero() || d.isNegative()) {
        throw new IllegalArgumentException(name + " must be positive, got " + d);
      }
      return d;
    }
  }
}
