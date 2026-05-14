package com.unifieddataprocessing.pubsub.schema;

/**
 * Thrown by {@link SchemaRegistry#get(String, int)} when the subject has no registered versions.
 */
public class SubjectNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public SubjectNotFoundException(String subject) {
    super("schema subject not found: " + subject);
  }
}
