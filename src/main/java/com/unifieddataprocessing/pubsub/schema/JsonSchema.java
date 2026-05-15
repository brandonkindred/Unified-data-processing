package com.unifieddataprocessing.pubsub.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unifieddataprocessing.pubsub.Message;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link Schema} implementation that requires the payload to be a JSON object containing a fixed
 * set of named fields, each of an expected {@link JsonFieldType}. Extra fields are allowed
 * (lenient). Instances are built via {@link #builder()} and are immutable and thread-safe.
 */
public final class JsonSchema implements Schema {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, JsonFieldType> requiredFields;

  private JsonSchema(Map<String, JsonFieldType> requiredFields) {
    this.requiredFields = Collections.unmodifiableMap(new LinkedHashMap<>(requiredFields));
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Returns the required-field map in declaration order. */
  public Map<String, JsonFieldType> requiredFields() {
    return requiredFields;
  }

  @Override
  public void validate(Message message) throws SchemaValidationException {
    Objects.requireNonNull(message, "message");
    String topic = message.getTopic();
    byte[] payload = message.getPayload();

    if (payload.length == 0) {
      throw new SchemaValidationException(topic, "empty payload");
    }

    JsonNode root;
    try {
      root = MAPPER.readTree(payload);
    } catch (JsonProcessingException e) {
      throw new SchemaValidationException(topic, "non-JSON payload: " + e.getOriginalMessage());
    } catch (IOException e) {
      throw new SchemaValidationException(topic, "non-JSON payload: " + e.getMessage());
    }

    if (root == null || root.isMissingNode() || root.isNull()) {
      throw new SchemaValidationException(topic, "non-JSON payload: empty document");
    }

    if (!root.isObject()) {
      JsonFieldType actual = JsonFieldType.of(root);
      throw new SchemaValidationException(
          topic, "expected JSON object at root, got " + (actual == null ? "NULL" : actual));
    }

    for (Map.Entry<String, JsonFieldType> entry : requiredFields.entrySet()) {
      String name = entry.getKey();
      JsonFieldType expected = entry.getValue();
      JsonNode field = root.get(name);
      if (field == null || field.isMissingNode() || field.isNull()) {
        throw new SchemaValidationException(topic, name, "missing required field");
      }
      JsonFieldType actual = JsonFieldType.of(field);
      if (actual != expected) {
        throw new SchemaValidationException(
            topic, name, "expected " + expected + ", got " + (actual == null ? "NULL" : actual));
      }
    }
  }

  /** Fluent builder for {@link JsonSchema}. Re-declaring a field overwrites its expected type. */
  public static final class Builder {

    private final Map<String, JsonFieldType> fields = new LinkedHashMap<>();

    private Builder() {}

    public Builder requireString(String name) {
      return require(name, JsonFieldType.STRING);
    }

    public Builder requireNumber(String name) {
      return require(name, JsonFieldType.NUMBER);
    }

    public Builder requireBoolean(String name) {
      return require(name, JsonFieldType.BOOLEAN);
    }

    public Builder requireObject(String name) {
      return require(name, JsonFieldType.OBJECT);
    }

    public Builder requireArray(String name) {
      return require(name, JsonFieldType.ARRAY);
    }

    private Builder require(String name, JsonFieldType type) {
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("field name must be non-blank");
      }
      fields.put(name, type);
      return this;
    }

    public JsonSchema build() {
      return new JsonSchema(fields);
    }
  }
}
