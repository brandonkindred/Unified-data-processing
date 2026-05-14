package com.unifieddataprocessing.pubsub.schema;

/**
 * Validates a message payload against a {@link Schema}.
 *
 * <p>Implementations are expected to be stateless and thread-safe — the data bridge invokes this
 * concurrently from N per-registration worker threads. A validator that needs to interpret the
 * schema body (for example a JSON Schema or Avro implementation) should dispatch on {@link
 * Schema#type()}; for simple deployments that only care about, say, payload size or magic prefixes,
 * a validator can ignore the schema entirely.
 *
 * <p>The returned {@link ValidationResult} must never be {@code null}.
 */
@FunctionalInterface
public interface SchemaValidator {

  ValidationResult validate(Schema schema, byte[] payload);
}
