package com.unifieddataprocessing.pubsub.schema;

/**
 * Thrown by {@link SchemaRegistry#get(String, int)} when the subject exists but the requested
 * version does not.
 */
public class VersionNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public VersionNotFoundException(String subject, int version) {
    super("schema version not found: subject=" + subject + " version=" + version);
  }
}
