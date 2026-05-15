package com.unifieddataprocessing.pubsub.schema;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Single source of truth for the JSON shape of a {@link JsonSchema}. Used identically by REST
 * request/response bodies and the Postgres {@code definition} JSONB column so the two
 * representations cannot drift.
 *
 * <p>Shape:
 *
 * <pre>{ "requiredFields": [ { "name": "userId", "type": "STRING" } ] }</pre>
 *
 * <p>All malformed inputs throw {@link IllegalArgumentException} — matching the precedent in
 * {@link JsonSchema.Builder} where invalid field inputs throw {@code IllegalArgumentException}.
 */
public final class SchemaCodec {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String REQUIRED_FIELDS = "requiredFields";
  private static final String NAME = "name";
  private static final String TYPE = "type";

  private SchemaCodec() {}

  /** Serialises {@code schema} to the canonical JSON shape. Field order is preserved. */
  public static String toJson(JsonSchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("schema must not be null");
    }
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode fields = root.putArray(REQUIRED_FIELDS);
    for (Map.Entry<String, JsonFieldType> entry : schema.requiredFields().entrySet()) {
      ObjectNode field = fields.addObject();
      field.put(NAME, entry.getKey());
      field.put(TYPE, entry.getValue().name());
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to serialise schema", e);
    }
  }

  /**
   * Parses the canonical JSON shape into a {@link JsonSchema}. Throws {@link
   * IllegalArgumentException} on any malformed input: invalid JSON, wrong root shape, missing
   * {@code requiredFields}, missing/non-textual {@code name} or {@code type}, unknown type, or
   * duplicate field names.
   */
  public static JsonSchema fromJson(String json) {
    if (json == null) {
      throw new IllegalArgumentException("json must not be null");
    }
    JsonNode root;
    try (JsonParser parser = MAPPER.getFactory().createParser(json)) {
      root = MAPPER.readTree(parser);
      if (root == null || root.isMissingNode()) {
        throw new IllegalArgumentException("invalid JSON: empty document");
      }
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("invalid JSON: trailing content after schema document");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("invalid JSON: " + e.getOriginalMessage(), e);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid JSON: " + e.getMessage(), e);
    }
    if (!root.isObject()) {
      throw new IllegalArgumentException("schema must be a JSON object");
    }
    JsonNode fields = root.get(REQUIRED_FIELDS);
    if (fields == null) {
      throw new IllegalArgumentException("missing 'requiredFields'");
    }
    if (!fields.isArray()) {
      throw new IllegalArgumentException("'requiredFields' must be a JSON array");
    }

    JsonSchema.Builder builder = JsonSchema.builder();
    Set<String> seen = new HashSet<>();
    for (JsonNode field : fields) {
      if (!field.isObject()) {
        throw new IllegalArgumentException("requiredFields entry must be a JSON object");
      }
      JsonNode nameNode = field.get(NAME);
      JsonNode typeNode = field.get(TYPE);
      if (nameNode == null || !nameNode.isTextual() || nameNode.asText().isBlank()) {
        throw new IllegalArgumentException("requiredFields entry missing non-blank 'name'");
      }
      if (typeNode == null || !typeNode.isTextual()) {
        throw new IllegalArgumentException("requiredFields entry missing textual 'type'");
      }
      String name = nameNode.asText();
      String typeText = typeNode.asText();
      if (!seen.add(name)) {
        throw new IllegalArgumentException("duplicate field name: " + name);
      }
      JsonFieldType type;
      try {
        type = JsonFieldType.valueOf(typeText);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("unknown field type: " + typeText, e);
      }
      switch (type) {
        case STRING -> builder.requireString(name);
        case NUMBER -> builder.requireNumber(name);
        case BOOLEAN -> builder.requireBoolean(name);
        case OBJECT -> builder.requireObject(name);
        case ARRAY -> builder.requireArray(name);
        default -> throw new IllegalArgumentException("unhandled field type: " + type);
      }
    }
    return builder.build();
  }
}
