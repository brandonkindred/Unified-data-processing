package com.unifieddataprocessing.pubsub.schema;

import com.fasterxml.jackson.databind.JsonNode;

/** Coarse JSON type tags used by {@link JsonSchema} to express required-field expectations. */
public enum JsonFieldType {
  STRING,
  NUMBER,
  BOOLEAN,
  OBJECT,
  ARRAY;

  /**
   * Returns the {@link JsonFieldType} that best describes {@code node}, or {@code null} if the node
   * is {@code null}, missing, or a JSON {@code null} literal.
   */
  static JsonFieldType of(JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return null;
    }
    if (node.isTextual()) {
      return STRING;
    }
    if (node.isNumber()) {
      return NUMBER;
    }
    if (node.isBoolean()) {
      return BOOLEAN;
    }
    if (node.isObject()) {
      return OBJECT;
    }
    if (node.isArray()) {
      return ARRAY;
    }
    return null;
  }
}
