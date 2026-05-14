package com.unifieddataprocessing.pubsub.relay;

/**
 * Attribute keys set by the data relay on every republished {@link
 * com.unifieddataprocessing.pubsub.Message} so downstream consumers can trace which relay
 * destination forwarded the message and from which unified-backbone topic.
 *
 * <p>Set in addition to (and without overwriting) the {@link
 * com.unifieddataprocessing.pubsub.bridge.BridgeAttributes} provenance stamped by the inbound
 * {@code DataBridge}, so a downstream consumer can recover the full path: original source → bridge
 * channel → relay destination.
 */
public final class RelayAttributes {

  /** Identifier of the relay destination registered with the relay (e.g. {@code "rabbit-prod"}). */
  public static final String RELAY_DESTINATION_ID = "relay.destinationId";

  /**
   * Topic on the unified backbone the relay read the message from (e.g. {@code "shopify.orders"}).
   */
  public static final String RELAY_SOURCE_TOPIC = "relay.sourceTopic";

  /** Topic on the downstream broker the relay republished the message to. */
  public static final String RELAY_DOWNSTREAM_TOPIC = "relay.downstreamTopic";

  private RelayAttributes() {}
}
