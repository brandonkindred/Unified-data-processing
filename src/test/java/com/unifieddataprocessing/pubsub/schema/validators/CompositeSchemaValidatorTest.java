package com.unifieddataprocessing.pubsub.schema.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import org.junit.jupiter.api.Test;

class CompositeSchemaValidatorTest {

  private final SchemaValidator alwaysOk = (s, p) -> ValidationResult.ok();
  private final SchemaValidator alwaysFail =
      (s, p) -> ValidationResult.fail("bad-" + s.type());

  @Test
  void dispatchesToMatchingTypeValidator() {
    CompositeSchemaValidator composite =
        CompositeSchemaValidator.builder()
            .forType("JSON", alwaysOk)
            .forType("AVRO", alwaysFail)
            .build();

    Schema jsonSchema = new Schema("orders", 1, "JSON", "{}");
    Schema avroSchema = new Schema("orders", 2, "AVRO", "schema");

    assertTrue(composite.validate(jsonSchema, new byte[0]).valid());
    ValidationResult avroResult = composite.validate(avroSchema, new byte[0]);
    assertFalse(avroResult.valid());
    assertEquals("bad-AVRO", avroResult.errors().get(0));
  }

  @Test
  void unknownType_withoutFallback_fails() {
    CompositeSchemaValidator composite =
        CompositeSchemaValidator.builder().forType("JSON", alwaysOk).build();
    Schema unknown = new Schema("orders", 1, "PROTOBUF", "schema");
    ValidationResult r = composite.validate(unknown, new byte[0]);
    assertFalse(r.valid());
    assertTrue(r.errors().get(0).contains("PROTOBUF"));
  }

  @Test
  void unknownType_usesFallback_whenPresent() {
    CompositeSchemaValidator composite =
        CompositeSchemaValidator.builder()
            .forType("JSON", alwaysFail)
            .fallback(alwaysOk)
            .build();
    Schema unknown = new Schema("orders", 1, "PROTOBUF", "schema");
    assertTrue(composite.validate(unknown, new byte[0]).valid());
  }

  @Test
  void laterRegistration_overridesEarlier() {
    CompositeSchemaValidator composite =
        CompositeSchemaValidator.builder()
            .forType("JSON", alwaysFail)
            .forType("JSON", alwaysOk)
            .build();
    Schema jsonSchema = new Schema("orders", 1, "JSON", "{}");
    assertTrue(composite.validate(jsonSchema, new byte[0]).valid());
  }
}
