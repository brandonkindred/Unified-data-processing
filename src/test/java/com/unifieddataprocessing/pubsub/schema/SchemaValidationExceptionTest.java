package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SchemaValidationExceptionTest {

  @Test
  void topicAndReasonCtor_getMessageContainsBoth_fieldNameIsNull() {
    SchemaValidationException e = new SchemaValidationException("orders", "empty payload");

    assertEquals("orders", e.topic());
    assertNull(e.fieldName());
    assertEquals("empty payload", e.reason());
    assertTrue(e.getMessage().contains("topic=orders"));
    assertTrue(e.getMessage().contains("reason=empty payload"));
    assertFalse(e.getMessage().contains("field="));
  }

  @Test
  void topicFieldReasonCtor_getMessageContainsAll() {
    SchemaValidationException e =
        new SchemaValidationException("orders", "amount", "expected NUMBER, got STRING");

    assertEquals("orders", e.topic());
    assertEquals("amount", e.fieldName());
    assertEquals("expected NUMBER, got STRING", e.reason());
    assertTrue(e.getMessage().contains("topic=orders"));
    assertTrue(e.getMessage().contains("field=amount"));
    assertTrue(e.getMessage().contains("reason=expected NUMBER, got STRING"));
  }

  @Test
  void nullTopic_throwsNpe() {
    assertThrows(NullPointerException.class, () -> new SchemaValidationException(null, "r"));
  }

  @Test
  void nullReason_throwsNpe() {
    assertThrows(
        NullPointerException.class, () -> new SchemaValidationException("orders", "field", null));
  }
}
