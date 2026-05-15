package com.unifieddataprocessing.pubsub.schema;

import java.util.Optional;
import java.util.Set;

/**
 * Topic-to-{@link Schema} lookup. Implementations decide their backing store (in-memory, remote
 * registry, etc.) and concurrency guarantees.
 */
public interface SchemaRegistry {

  /** Associates {@code schema} with {@code topic}, overwriting any prior registration. */
  void register(String topic, Schema schema);

  /** Removes any schema registered for {@code topic}; no-op if none. */
  void unregister(String topic);

  /** Returns the schema bound to {@code topic}, if one is registered. */
  Optional<Schema> findSchema(String topic);

  /** Returns an immutable snapshot of currently registered topics. */
  Set<String> listTopics();
}
