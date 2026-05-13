package com.unifieddataprocessing.pubsub.relay;

import com.unifieddataprocessing.pubsub.Message;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Rewrites a backbone {@link Message} into the form republished onto the registration's downstream
 * broker: the topic becomes the registration's downstream topic and the relay-owned provenance
 * attributes are stamped onto the message, overwriting any caller-supplied values.
 *
 * <p>Any pre-existing attributes (including the inbound {@code BridgeAttributes} provenance set by
 * the {@code DataBridge}) are preserved, so downstream consumers can recover the full path.
 */
final class RelayMessageRewriter {

  private RelayMessageRewriter() {}

  static Message rewrite(Message src, RelayRegistration reg) {
    Map<String, String> attrs = new LinkedHashMap<>(src.getAttributes());
    attrs.put(RelayAttributes.RELAY_DESTINATION_ID, reg.destinationId());
    attrs.put(RelayAttributes.RELAY_SOURCE_TOPIC, reg.sourceTopic());
    attrs.put(RelayAttributes.RELAY_DOWNSTREAM_TOPIC, reg.downstreamTopic());
    return new Message(src.getId(), reg.downstreamTopic(), src.getPayload(), attrs);
  }
}
