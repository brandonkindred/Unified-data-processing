package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaCodecTest {

  @Test
  void toJson_emitsExpectedShape() {
    JsonSchema schema =
        JsonSchema.builder().requireString("userId").requireNumber("count").build();

    String json = SchemaCodec.toJson(schema);

    assertEquals(
        "{\"requiredFields\":[{\"name\":\"userId\",\"type\":\"STRING\"},"
            + "{\"name\":\"count\",\"type\":\"NUMBER\"}]}",
        json);
  }

  @Test
  void roundtrip_preservesFieldOrder() {
    JsonSchema original =
        JsonSchema.builder()
            .requireString("zeta")
            .requireBoolean("alpha")
            .requireArray("mid")
            .build();

    String first = SchemaCodec.toJson(original);
    JsonSchema decoded = SchemaCodec.fromJson(first);
    String second = SchemaCodec.toJson(decoded);

    assertEquals(first, second);

    Iterator<Map.Entry<String, JsonFieldType>> it = decoded.requiredFields().entrySet().iterator();
    Map.Entry<String, JsonFieldType> e1 = it.next();
    assertEquals("zeta", e1.getKey());
    assertEquals(JsonFieldType.STRING, e1.getValue());
    Map.Entry<String, JsonFieldType> e2 = it.next();
    assertEquals("alpha", e2.getKey());
    assertEquals(JsonFieldType.BOOLEAN, e2.getValue());
    Map.Entry<String, JsonFieldType> e3 = it.next();
    assertEquals("mid", e3.getKey());
    assertEquals(JsonFieldType.ARRAY, e3.getValue());
  }

  @Test
  void fromJson_unknownType_rejected() {
    String json = "{\"requiredFields\":[{\"name\":\"x\",\"type\":\"FLOAT\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("unknown field type"), () -> "message=" + e.getMessage());
    assertTrue(e.getMessage().contains("FLOAT"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_duplicateFieldName_rejected() {
    String json =
        "{\"requiredFields\":["
            + "{\"name\":\"id\",\"type\":\"STRING\"},"
            + "{\"name\":\"id\",\"type\":\"NUMBER\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("duplicate"), () -> "message=" + e.getMessage());
    assertTrue(e.getMessage().contains("id"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_missingName_rejected() {
    String json = "{\"requiredFields\":[{\"type\":\"STRING\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("name"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_missingType_rejected() {
    String json = "{\"requiredFields\":[{\"name\":\"x\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("type"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_missingRequiredFields_rejected() {
    String json = "{}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("requiredFields"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_emptyRequiredFields_isAllowed() {
    String json = "{\"requiredFields\":[]}";

    JsonSchema schema = SchemaCodec.fromJson(json);

    assertTrue(schema.requiredFields().isEmpty());
    assertEquals("{\"requiredFields\":[]}", SchemaCodec.toJson(schema));
  }

  @Test
  void fromJson_invalidJson_rejected() {
    assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson("{"));
    assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson("not json"));
  }

  @Test
  void fromJson_nonObjectRoot_rejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCodec.fromJson("[{\"name\":\"x\",\"type\":\"STRING\"}]"));
    assertTrue(e.getMessage().contains("JSON object"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_requiredFieldsNotArray_rejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCodec.fromJson("{\"requiredFields\":\"not-an-array\"}"));
    assertTrue(e.getMessage().contains("array"), () -> "message=" + e.getMessage());
  }

  @Test
  void fromJson_nameNotTextual_rejected() {
    String json = "{\"requiredFields\":[{\"name\":42,\"type\":\"STRING\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(json));
    assertTrue(e.getMessage().contains("name"), () -> "message=" + e.getMessage());
  }

  @Test
  void toJson_nullSchema_rejected() {
    assertThrows(IllegalArgumentException.class, () -> SchemaCodec.toJson(null));
  }

  @Test
  void fromJson_nullJson_rejected() {
    assertThrows(IllegalArgumentException.class, () -> SchemaCodec.fromJson(null));
  }
}
