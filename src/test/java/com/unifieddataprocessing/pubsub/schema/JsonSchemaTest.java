package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.unifieddataprocessing.pubsub.Message;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonSchemaTest {

  private static final String TOPIC = "orders";

  @Test
  void validate_allRequiredFieldsPresent_passes() {
    JsonSchema schema =
        JsonSchema.builder()
            .requireString("id")
            .requireNumber("amount")
            .requireBoolean("paid")
            .build();
    Message m = message("{\"id\":\"a-1\",\"amount\":42,\"paid\":true}");

    assertDoesNotThrow(() -> schema.validate(m));
  }

  @Test
  void validate_missingField_throwsWithFieldName() {
    JsonSchema schema = JsonSchema.builder().requireString("id").requireNumber("amount").build();
    Message m = message("{\"id\":\"a-1\"}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertEquals("amount", e.fieldName());
    assertTrue(e.reason().contains("missing"), () -> "reason=" + e.reason());
    assertEquals(TOPIC, e.topic());
  }

  @Test
  void validate_wrongType_throwsWithExpectedAndActual() {
    JsonSchema schema = JsonSchema.builder().requireString("id").build();
    Message m = message("{\"id\":123}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertEquals("id", e.fieldName());
    assertTrue(e.reason().contains("expected STRING"), () -> "reason=" + e.reason());
    assertTrue(e.reason().contains("got NUMBER"), () -> "reason=" + e.reason());
  }

  @Test
  void validate_booleanMismatch_throws() {
    JsonSchema schema = JsonSchema.builder().requireBoolean("paid").build();
    Message m = message("{\"paid\":\"yes\"}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertTrue(e.reason().contains("expected BOOLEAN"));
    assertTrue(e.reason().contains("got STRING"));
  }

  @Test
  void validate_objectMismatch_throws() {
    JsonSchema schema = JsonSchema.builder().requireObject("details").build();
    Message m = message("{\"details\":[1,2]}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertTrue(e.reason().contains("expected OBJECT"));
    assertTrue(e.reason().contains("got ARRAY"));
  }

  @Test
  void validate_arrayMismatch_throws() {
    JsonSchema schema = JsonSchema.builder().requireArray("tags").build();
    Message m = message("{\"tags\":{\"x\":1}}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertTrue(e.reason().contains("expected ARRAY"));
    assertTrue(e.reason().contains("got OBJECT"));
  }

  @Test
  void validate_nonJsonPayload_throws() {
    JsonSchema schema = JsonSchema.builder().requireString("id").build();
    Message m = message("not json at all");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertNull(e.fieldName());
    assertTrue(e.reason().contains("non-JSON"), () -> "reason=" + e.reason());
  }

  @Test
  void validate_emptyPayload_throws() {
    JsonSchema schema = JsonSchema.builder().requireString("id").build();
    Message m = new Message("id-1", TOPIC, new byte[0], Map.of());

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertNull(e.fieldName());
    assertEquals("empty payload", e.reason());
  }

  @Test
  void validate_arrayRootPayload_throws() {
    JsonSchema schema = JsonSchema.builder().requireString("id").build();
    Message m = message("[1,2,3]");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertNull(e.fieldName());
    assertTrue(e.reason().contains("expected JSON object"), () -> "reason=" + e.reason());
    assertTrue(e.reason().contains("ARRAY"), () -> "reason=" + e.reason());
  }

  @Test
  void validate_extraFields_passes() {
    JsonSchema schema = JsonSchema.builder().requireString("a").build();
    Message m = message("{\"a\":\"x\",\"b\":99,\"c\":[1,2]}");

    assertDoesNotThrow(() -> schema.validate(m));
  }

  @Test
  void validate_nullJsonField_treatedAsMissing() {
    JsonSchema schema = JsonSchema.builder().requireString("id").build();
    Message m = message("{\"id\":null}");

    SchemaValidationException e =
        assertThrows(SchemaValidationException.class, () -> schema.validate(m));
    assertEquals("id", e.fieldName());
    assertTrue(e.reason().contains("missing"));
  }

  @Test
  void builder_blankName_throws() {
    assertThrows(IllegalArgumentException.class, () -> JsonSchema.builder().requireString(""));
    assertThrows(IllegalArgumentException.class, () -> JsonSchema.builder().requireNumber(null));
  }

  private static Message message(String json) {
    return new Message("id-1", TOPIC, json.getBytes(StandardCharsets.UTF_8), Map.of());
  }
}
