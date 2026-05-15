package com.unifieddataprocessing.pubsub.schema;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe in-process {@link SchemaRegistry}. Backed by a {@link ConcurrentHashMap}; concurrent
 * {@link #register} calls publish their values with happens-before semantics for subsequent
 * {@link #findSchema} and {@link #listTopics} readers.
 */
public final class InMemorySchemaRegistry implements SchemaRegistry {

  private final ConcurrentMap<String, Schema> schemasByTopic = new ConcurrentHashMap<>();

  @Override
  public void register(String topic, Schema schema) {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(schema, "schema");
    schemasByTopic.put(topic, schema);
  }

  @Override
  public void unregister(String topic) {
    Objects.requireNonNull(topic, "topic");
    schemasByTopic.remove(topic);
  }

  @Override
  public Optional<Schema> findSchema(String topic) {
    Objects.requireNonNull(topic, "topic");
    return Optional.ofNullable(schemasByTopic.get(topic));
  }

  @Override
  public Set<String> listTopics() {
    return Set.copyOf(schemasByTopic.keySet());
  }
}
