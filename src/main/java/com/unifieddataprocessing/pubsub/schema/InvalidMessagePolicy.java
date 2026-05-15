package com.unifieddataprocessing.pubsub.schema;

/**
 * How a consumer or bridge should react when {@link Schema#validate} rejects a message. Consumed
 * by later bridge/relay wiring; not used by the in-process registry itself.
 */
public enum InvalidMessagePolicy {

  /** Drop the offending message and continue processing the batch. */
  SKIP,

  /** Surface the failure to the caller so the batch aborts. */
  FAIL
}
