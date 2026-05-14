package com.unifieddataprocessing.pubsub.schema.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import org.junit.jupiter.api.Test;

class MaxPayloadSizeSchemaValidatorTest {

  private final Schema schema = new Schema("orders", 1, "JSON", "{}");

  @Test
  void payloadAtLimit_passes() {
    MaxPayloadSizeSchemaValidator v = new MaxPayloadSizeSchemaValidator(4);
    ValidationResult r = v.validate(schema, new byte[] {1, 2, 3, 4});
    assertTrue(r.valid());
  }

  @Test
  void payloadOverLimit_fails() {
    MaxPayloadSizeSchemaValidator v = new MaxPayloadSizeSchemaValidator(3);
    ValidationResult r = v.validate(schema, new byte[] {1, 2, 3, 4});
    assertFalse(r.valid());
    assertEquals(1, r.errors().size());
    assertTrue(r.errors().get(0).contains("exceeds max"));
  }

  @Test
  void emptyPayloadWithZeroLimit_passes() {
    MaxPayloadSizeSchemaValidator v = new MaxPayloadSizeSchemaValidator(0);
    ValidationResult r = v.validate(schema, new byte[0]);
    assertTrue(r.valid());
  }

  @Test
  void negativeLimit_rejectedByConstructor() {
    assertThrows(
        IllegalArgumentException.class, () -> new MaxPayloadSizeSchemaValidator(-1));
  }

  @Test
  void nullPayload_throws() {
    MaxPayloadSizeSchemaValidator v = new MaxPayloadSizeSchemaValidator(10);
    assertThrows(NullPointerException.class, () -> v.validate(schema, null));
  }
}
