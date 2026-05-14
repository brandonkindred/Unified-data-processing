package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ValidationResultTest {

  @Test
  void valid_factory_hasNoErrors() {
    ValidationResult r = ValidationResult.ok();
    assertTrue(r.valid());
    assertTrue(r.errors().isEmpty());
  }

  @Test
  void invalid_singleError_factory() {
    ValidationResult r = ValidationResult.fail("boom");
    assertFalse(r.valid());
    assertEquals(List.of("boom"), r.errors());
  }

  @Test
  void invalid_listErrors_factory() {
    ValidationResult r = ValidationResult.fail(List.of("a", "b"));
    assertFalse(r.valid());
    assertEquals(List.of("a", "b"), r.errors());
  }

  @Test
  void errorsAreDefensivelyCopied() {
    List<String> errors = new ArrayList<>();
    errors.add("first");
    ValidationResult r = new ValidationResult(false, errors);
    errors.add("second");
    assertEquals(List.of("first"), r.errors());
  }

  @Test
  void errorsList_isUnmodifiable() {
    ValidationResult r = ValidationResult.fail("x");
    assertThrows(UnsupportedOperationException.class, () -> r.errors().add("y"));
  }

  @Test
  void nullErrors_rejected() {
    assertThrows(NullPointerException.class, () -> new ValidationResult(false, null));
  }
}
