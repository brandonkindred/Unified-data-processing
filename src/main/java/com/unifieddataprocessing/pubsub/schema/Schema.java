package com.unifieddataprocessing.pubsub.schema;

import com.unifieddataprocessing.pubsub.Message;

/**
 * Validates a {@link Message} against a topic-bound contract. Implementations decide payload
 * encoding (JSON, Avro, etc.) and which structural rules apply.
 */
public interface Schema {

  /**
   * Validates {@code message} and returns normally if it conforms.
   *
   * @throws SchemaValidationException if the payload violates the schema
   */
  void validate(Message message) throws SchemaValidationException;
}
