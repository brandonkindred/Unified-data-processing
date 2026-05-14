package com.unifieddataprocessing.pubsub.schema.validators;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import org.junit.jupiter.api.Test;

class PermissiveSchemaValidatorTest {

  private final PermissiveSchemaValidator validator = new PermissiveSchemaValidator();
  private final Schema schema = new Schema("orders", 1, "JSON", "{}");

  @Test
  void anyPayload_passes() {
    ValidationResult r = validator.validate(schema, new byte[] {1, 2, 3});
    assertTrue(r.valid());
    assertTrue(r.errors().isEmpty());
  }

  @Test
  void emptyPayload_passes() {
    ValidationResult r = validator.validate(schema, new byte[0]);
    assertTrue(r.valid());
  }
}
