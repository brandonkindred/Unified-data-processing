package com.unifieddataprocessing.pubsub.bridge;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-channel topic-provisioning overrides for a bridge registration.
 *
 * <p>A value of {@code 0} for {@code partitions} or {@code replicationFactor} is a sentinel meaning
 * "fall back to the corresponding {@code DataBridgeConfig} default". Negative values are rejected
 * by the builder.
 */
public final class ChannelOptions {

  private final int partitions;
  private final short replicationFactor;
  private final Map<String, String> topicConfigs;

  private ChannelOptions(
      int partitions, short replicationFactor, Map<String, String> topicConfigs) {
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.topicConfigs = topicConfigs;
  }

  /**
   * Returns options that defer entirely to {@code DataBridgeConfig}: {@code partitions=0},
   * {@code replicationFactor=0}, empty {@code topicConfigs}.
   */
  public static ChannelOptions defaults() {
    return new ChannelOptions(0, (short) 0, Collections.emptyMap());
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getPartitions() {
    return partitions;
  }

  public short getReplicationFactor() {
    return replicationFactor;
  }

  public Map<String, String> getTopicConfigs() {
    return topicConfigs;
  }

  /** Fluent builder for {@link ChannelOptions}. */
  public static final class Builder {

    private int partitions;
    private short replicationFactor;
    private final Map<String, String> topicConfigs = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Sets the per-channel partition count. {@code 0} is a sentinel meaning "use the bridge
     * default"; negative values are rejected.
     */
    public Builder partitions(int partitions) {
      if (partitions < 0) {
        throw new IllegalArgumentException("partitions must be >= 0, got " + partitions);
      }
      this.partitions = partitions;
      return this;
    }

    /**
     * Sets the per-channel replication factor. {@code 0} is a sentinel meaning "use the bridge
     * default"; negative values are rejected.
     */
    public Builder replicationFactor(short replicationFactor) {
      if (replicationFactor < 0) {
        throw new IllegalArgumentException(
            "replicationFactor must be >= 0, got " + replicationFactor);
      }
      this.replicationFactor = replicationFactor;
      return this;
    }

    /** Replaces all accumulated entries with a defensive copy of {@code topicConfigs}. */
    public Builder topicConfigs(Map<String, String> topicConfigs) {
      Objects.requireNonNull(topicConfigs, "topicConfigs");
      this.topicConfigs.clear();
      for (Map.Entry<String, String> e : topicConfigs.entrySet()) {
        topicConfig(e.getKey(), e.getValue());
      }
      return this;
    }

    /** Adds a single Kafka topic-config entry (e.g. {@code retention.ms}). */
    public Builder topicConfig(String key, String value) {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(value, "value");
      this.topicConfigs.put(key, value);
      return this;
    }

    /** Builds an immutable {@link ChannelOptions} snapshot. */
    public ChannelOptions build() {
      Map<String, String> snapshot =
          topicConfigs.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(topicConfigs));
      return new ChannelOptions(partitions, replicationFactor, snapshot);
    }
  }
}
