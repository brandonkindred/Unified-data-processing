package com.unifieddataprocessing.pubsub.schema.validators;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;

/**
 * {@link SchemaValidator} that accepts every payload.
 *
 * <p>Useful as a {@link CompositeSchemaValidator} fallback for schema types the deployment does not
 * yet know how to enforce, or as a no-op in tests that exercise schema-registration plumbing
 * without caring about payload contents.
 */
public final class PermissiveSchemaValidator implements SchemaValidator {

  @Override
  public ValidationResult validate(Schema schema, byte[] payload) {
    return ValidationResult.ok();
  }
}
