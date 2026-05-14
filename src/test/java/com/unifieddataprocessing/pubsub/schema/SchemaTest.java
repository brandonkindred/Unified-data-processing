package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SchemaTest {

  @Test
  void validInputs_construct() {
    Schema s = new Schema("orders", 1, "JSON", "{\"type\":\"object\"}");
    assertEquals("orders", s.subject());
    assertEquals(1, s.version());
    assertEquals("JSON", s.type());
    assertEquals("{\"type\":\"object\"}", s.definition());
  }

  @Test
  void emptyDefinition_allowed() {
    assertDoesNotThrow(() -> new Schema("orders", 1, "NONE", ""));
  }

  @Test
  void blankSubject_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema(" ", 1, "JSON", "{}"));
  }

  @Test
  void nullSubject_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema(null, 1, "JSON", "{}"));
  }

  @Test
  void blankType_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema("s", 1, " ", "{}"));
  }

  @Test
  void nullType_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema("s", 1, null, "{}"));
  }

  @Test
  void versionZero_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema("s", 0, "JSON", "{}"));
  }

  @Test
  void negativeVersion_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new Schema("s", -1, "JSON", "{}"));
  }

  @Test
  void nullDefinition_rejected() {
    assertThrows(NullPointerException.class, () -> new Schema("s", 1, "JSON", null));
  }
}
