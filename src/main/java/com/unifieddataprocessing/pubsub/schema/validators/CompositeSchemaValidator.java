package com.unifieddataprocessing.pubsub.schema.validators;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link SchemaValidator} that dispatches to a per-type {@link SchemaValidator} based on {@link
 * Schema#type()}.
 *
 * <p>Build with {@link #builder()} — register one validator per type label your registry uses
 * ({@code "JSON"}, {@code "AVRO"}, ...) and optionally a fallback for unknown types. If no
 * fallback is configured, an unknown type returns a failing {@link ValidationResult} so silent
 * mis-routing is impossible.
 */
public final class CompositeSchemaValidator implements SchemaValidator {

  private final Map<String, SchemaValidator> byType;
  private final SchemaValidator fallback;

  private CompositeSchemaValidator(Map<String, SchemaValidator> byType, SchemaValidator fallback) {
    this.byType = byType;
    this.fallback = fallback;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ValidationResult validate(Schema schema, byte[] payload) {
    Objects.requireNonNull(schema, "schema");
    SchemaValidator validator = byType.get(schema.type());
    if (validator != null) {
      return validator.validate(schema, payload);
    }
    if (fallback != null) {
      return fallback.validate(schema, payload);
    }
    return ValidationResult.fail("no validator registered for schema type: " + schema.type());
  }

  /** Fluent builder for {@link CompositeSchemaValidator}. */
  public static final class Builder {

    private final Map<String, SchemaValidator> byType = new LinkedHashMap<>();
    private SchemaValidator fallback;

    private Builder() {}

    /** Registers a validator for a specific {@link Schema#type()} label; overwrites duplicates. */
    public Builder forType(String type, SchemaValidator validator) {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(validator, "validator");
      byType.put(type, validator);
      return this;
    }

    /**
     * Sets the validator used when no per-type entry matches. If unset, unknown types fail
     * validation with a "no validator registered" error.
     */
    public Builder fallback(SchemaValidator fallback) {
      this.fallback = Objects.requireNonNull(fallback, "fallback");
      return this;
    }

    public CompositeSchemaValidator build() {
      return new CompositeSchemaValidator(Map.copyOf(byType), fallback);
    }
  }
}
