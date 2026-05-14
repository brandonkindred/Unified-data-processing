package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import com.unifieddataprocessing.pubsub.schema.SchemaRegistry;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.SchemaViolationPolicy;
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
  private final int publishFailureThreshold;
  private final Duration publishFailureCooldown;
  private final int defaultPartitions;
  private final short defaultReplicationFactor;
  private final SchemaRegistry schemaRegistry;
  private final SchemaValidator schemaValidator;
  private final SchemaViolationPolicy schemaViolationPolicy;

  private DataBridgeConfig(Builder b) {
    this.producerConfig = b.producerConfig;
    this.pollTimeout = b.pollTimeout;
    this.publishTimeout = b.publishTimeout;
    this.shutdownTimeout = b.shutdownTimeout;
    this.closeForceTimeout = b.closeForceTimeout;
    this.pollBackoff = b.pollBackoff;
    this.publishFailureThreshold = b.publishFailureThreshold;
    this.publishFailureCooldown = b.publishFailureCooldown;
    this.defaultPartitions = b.defaultPartitions;
    this.defaultReplicationFactor = b.defaultReplicationFactor;
    this.schemaRegistry = b.schemaRegistry;
    this.schemaValidator = b.schemaValidator;
    this.schemaViolationPolicy = b.schemaViolationPolicy;
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

  /**
   * Number of consecutive {@code publish(...)} failures on a single registration before the
   * bridge's per-registration circuit-breaker engages and pauses polling for
   * {@link #publishFailureCooldown()}. A successful publish resets the counter.
   */
  public int publishFailureThreshold() {
    return publishFailureThreshold;
  }

  /**
   * Sleep applied to the failing registration's worker once the circuit-breaker engages, before
   * the next {@code poll(...)} is attempted. Bounds the in-memory bookkeeping growth on
   * cursor-based sources during sustained downstream outages.
   */
  public Duration publishFailureCooldown() {
    return publishFailureCooldown;
  }

  public int defaultPartitions() {
    return defaultPartitions;
  }

  public short defaultReplicationFactor() {
    return defaultReplicationFactor;
  }

  /**
   * Registry consulted on every published message to look up the latest schema for the target
   * topic. {@code null} disables schema validation entirely. When non-null, {@link
   * #schemaValidator()} must also be non-null.
   */
  public SchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }

  /**
   * Validator the bridge invokes against the {@link
   * com.unifieddataprocessing.pubsub.schema.Schema} returned by {@link #schemaRegistry()} for each
   * outgoing message. {@code null} when schema validation is disabled.
   */
  public SchemaValidator schemaValidator() {
    return schemaValidator;
  }

  /**
   * Behavior the bridge applies when a message fails schema validation. Defaults to {@link
   * SchemaViolationPolicy#DROP}; ignored when {@link #schemaRegistry()} is {@code null}.
   */
  public SchemaViolationPolicy schemaViolationPolicy() {
    return schemaViolationPolicy;
  }

  /** Fluent builder for {@link DataBridgeConfig}. */
  public static final class Builder {

    private KafkaProducerConfig producerConfig;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration publishTimeout = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration closeForceTimeout = Duration.ofSeconds(5);
    private Duration pollBackoff = Duration.ofSeconds(1);
    private int publishFailureThreshold = 5;
    private Duration publishFailureCooldown = Duration.ofSeconds(30);
    private int defaultPartitions = 1;
    private short defaultReplicationFactor = 1;
    private SchemaRegistry schemaRegistry;
    private SchemaValidator schemaValidator;
    private SchemaViolationPolicy schemaViolationPolicy = SchemaViolationPolicy.DROP;

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

    /**
     * Number of consecutive {@code publish(...)} failures on a single registration that engages
     * the bridge's circuit-breaker. Must be > 0.
     */
    public Builder publishFailureThreshold(int publishFailureThreshold) {
      if (publishFailureThreshold <= 0) {
        throw new IllegalArgumentException(
            "publishFailureThreshold must be > 0, got " + publishFailureThreshold);
      }
      this.publishFailureThreshold = publishFailureThreshold;
      return this;
    }

    /**
     * Sleep applied to the failing registration's worker once the circuit-breaker engages, before
     * the next poll. Must be > 0.
     */
    public Builder publishFailureCooldown(Duration publishFailureCooldown) {
      this.publishFailureCooldown =
          requirePositive("publishFailureCooldown", publishFailureCooldown);
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

    /**
     * Registry the bridge consults on every published message. Setting this enables schema
     * validation on the publish hot path; {@link #schemaValidator(SchemaValidator)} must also be
     * set before {@link #build()}. Pass {@code null} to disable validation.
     */
    public Builder schemaRegistry(SchemaRegistry schemaRegistry) {
      this.schemaRegistry = schemaRegistry;
      return this;
    }

    /**
     * Validator the bridge invokes against each schema returned by {@link #schemaRegistry()}.
     * Required when a registry is set. Pass {@code null} to clear.
     */
    public Builder schemaValidator(SchemaValidator schemaValidator) {
      this.schemaValidator = schemaValidator;
      return this;
    }

    /**
     * Policy applied when a payload fails schema validation. Defaults to {@link
     * SchemaViolationPolicy#DROP}. Non-null.
     */
    public Builder schemaViolationPolicy(SchemaViolationPolicy schemaViolationPolicy) {
      this.schemaViolationPolicy =
          Objects.requireNonNull(schemaViolationPolicy, "schemaViolationPolicy");
      return this;
    }

    /** Builds an immutable {@link DataBridgeConfig}; throws if {@code producerConfig} is unset. */
    public DataBridgeConfig build() {
      if (producerConfig == null) {
        throw new IllegalStateException("producerConfig is required");
      }
      if (schemaRegistry != null && schemaValidator == null) {
        throw new IllegalStateException(
            "schemaValidator is required when schemaRegistry is set");
      }
      if (schemaValidator != null && schemaRegistry == null) {
        throw new IllegalStateException(
            "schemaRegistry is required when schemaValidator is set");
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
