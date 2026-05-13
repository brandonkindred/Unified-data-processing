package com.unifieddataprocessing.pubsub.relay;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable top-level configuration for the data relay.
 *
 * <p>Carries only timing knobs that govern poll/publish behavior; the relay is broker-agnostic on
 * both ends (each registration brings its own
 * {@link com.unifieddataprocessing.pubsub.PubSubConsumer} and
 * {@link com.unifieddataprocessing.pubsub.PubSubPublisher}), so there is no broker-specific
 * configuration to carry here. Build instances via {@link #builder()}; every duration must be
 * strictly positive.
 */
public final class DataRelayConfig {

  private final Duration pollTimeout;
  private final Duration publishTimeout;
  private final Duration shutdownTimeout;
  private final Duration closeForceTimeout;
  private final Duration pollBackoff;

  private DataRelayConfig(Builder b) {
    this.pollTimeout = b.pollTimeout;
    this.publishTimeout = b.publishTimeout;
    this.shutdownTimeout = b.shutdownTimeout;
    this.closeForceTimeout = b.closeForceTimeout;
    this.pollBackoff = b.pollBackoff;
  }

  public static Builder builder() {
    return new Builder();
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

  /** Fluent builder for {@link DataRelayConfig}. */
  public static final class Builder {

    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration publishTimeout = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration closeForceTimeout = Duration.ofSeconds(5);
    private Duration pollBackoff = Duration.ofSeconds(1);

    private Builder() {}

    /** Poll timeout passed to each registered backbone consumer. Must be strictly positive. */
    public Builder pollTimeout(Duration pollTimeout) {
      this.pollTimeout = requirePositive("pollTimeout", pollTimeout);
      return this;
    }

    /** Per-message publish timeout for the downstream publish future. Must be strictly positive. */
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
     * publishers/consumers. Must be strictly positive.
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

    /** Builds an immutable {@link DataRelayConfig}. */
    public DataRelayConfig build() {
      return new DataRelayConfig(this);
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
