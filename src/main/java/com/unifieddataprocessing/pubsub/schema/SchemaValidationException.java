package com.unifieddataprocessing.pubsub.schema;

import java.util.Objects;

/**
 * Thrown when a {@link Schema} rejects a message. Carries the topic, the offending field name (if
 * any), and a human-readable reason so callers can route, log, or surface diagnostics without
 * re-parsing the message body.
 */
public class SchemaValidationException extends Exception {

  private static final long serialVersionUID = 1L;

  private final String topic;
  private final String fieldName;
  private final String reason;

  /** Creates a schema-level failure (no specific field). */
  public SchemaValidationException(String topic, String reason) {
    this(topic, null, reason);
  }

  /**
   * Creates a field-level failure. {@code fieldName} may be {@code null} for a schema-level
   * error.
   */
  public SchemaValidationException(String topic, String fieldName, String reason) {
    super(buildMessage(topic, fieldName, reason));
    this.topic = Objects.requireNonNull(topic, "topic");
    this.fieldName = fieldName;
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  public String topic() {
    return topic;
  }

  public String fieldName() {
    return fieldName;
  }

  public String reason() {
    return reason;
  }

  private static String buildMessage(String topic, String fieldName, String reason) {
    StringBuilder sb = new StringBuilder("topic=").append(topic);
    if (fieldName != null) {
      sb.append(" field=").append(fieldName);
    }
    sb.append(" reason=").append(reason);
    return sb.toString();
  }
}
