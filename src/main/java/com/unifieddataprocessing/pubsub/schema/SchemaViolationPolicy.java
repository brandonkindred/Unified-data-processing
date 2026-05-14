package com.unifieddataprocessing.pubsub.schema;

/**
 * Behavior the data bridge applies when a message fails schema validation.
 *
 * <p>{@link #DROP} acks the source message and continues processing the batch — the violating
 * message is logged and discarded. Use when malformed messages should not block delivery of
 * well-formed traffic and replaying them serves no purpose.
 *
 * <p>{@link #FAIL} treats the violation like a publish failure: the source message is not acked,
 * the current batch is broken, and the bridge's per-registration circuit breaker counts the
 * failure. Use when malformed messages must be redelivered (for example until a schema update
 * makes them valid) or surfaced loudly to operators.
 */
public enum SchemaViolationPolicy {
  DROP,
  FAIL
}
