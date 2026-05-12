package com.unifieddataprocessing.pubsub.bridge;

import java.util.Map;

/**
 * Describes one Kafka topic that {@link BridgeTopicProvisioner} should attempt to create.
 *
 * <p>This is a dumb carrier — validation of {@code name}, {@code partitions}, and {@code
 * replicationFactor} happens at the boundary that produces the spec (the bridge's registration
 * code). {@code configs} must be non-null but may be empty.
 */
record NewTopicSpec(
    String name, int partitions, short replicationFactor, Map<String, String> configs) {}
