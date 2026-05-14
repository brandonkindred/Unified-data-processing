package com.unifieddataprocessing.pubsub.schema;

import java.util.Objects;

/**
 * Immutable record of one schema version stored in a {@link SchemaRegistry}.
 *
 * <p>A schema is identified by its {@code subject} — for the data bridge that is the target Kafka
 * topic name ({@code sourceId + "." + channel}). Each subject keeps an append-only history of
 * versions starting at {@code 1}; {@code type} is an opaque label that a {@link SchemaValidator}
 * uses to dispatch parsing (for example {@code "JSON"}, {@code "AVRO"}, {@code "PROTOBUF"}); {@code
 * definition} is the raw schema body that the validator interprets.
 *
 * <p>Validation runs in the compact constructor so a {@code Schema} can never exist in an invalid
 * state: {@code subject} and {@code type} must be non-blank, {@code version} must be {@code >= 1},
 * and {@code definition} must be non-null (an empty body is allowed for type labels that do not
 * need one).
 */
public record Schema(String subject, int version, String type, String definition) {

  /** Compact constructor: rejects blank identifiers, non-positive versions, and null bodies. */
  public Schema {
    if (subject == null || subject.isBlank()) {
      throw new IllegalArgumentException("subject must be non-blank");
    }
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("type must be non-blank");
    }
    if (version < 1) {
      throw new IllegalArgumentException("version must be >= 1, got " + version);
    }
    Objects.requireNonNull(definition, "definition");
  }
}
