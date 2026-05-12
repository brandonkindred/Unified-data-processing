package com.unifieddataprocessing.pubsub.bridge;

/**
 * Attribute keys set by the data bridge on every republished {@link
 * com.unifieddataprocessing.pubsub.Message} so downstream consumers can recover the original
 * source.
 */
public final class BridgeAttributes {

  /** Identifier of the source registered with the bridge (e.g. {@code "shopify"}). */
  public static final String BRIDGE_SOURCE_ID = "bridge.sourceId";

  /** Source-side topic / subscription name the message was originally read from. */
  public static final String BRIDGE_SOURCE_TOPIC = "bridge.sourceTopic";

  /** Logical channel name within the source (e.g. {@code "orders"}). */
  public static final String BRIDGE_CHANNEL = "bridge.channel";

  private BridgeAttributes() {}
}
