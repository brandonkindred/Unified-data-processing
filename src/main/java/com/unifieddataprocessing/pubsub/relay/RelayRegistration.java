package com.unifieddataprocessing.pubsub.relay;

import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import java.util.regex.Pattern;

/**
 * One registered backbone-to-destination binding inside a {@code DataRelay}.
 *
 * <p>Validation runs in the compact constructor so a {@code RelayRegistration} can never exist in
 * an invalid state. Each registration is a fully independent pipeline: its own
 * {@link PubSubConsumer} reading from {@code sourceTopic} on the unified backbone, and its own
 * {@link PubSubPublisher} writing to {@code downstreamTopic} on the destination broker.
 */
record RelayRegistration(
    String destinationId,
    String sourceTopic,
    String downstreamTopic,
    PubSubConsumer consumer,
    PubSubPublisher publisher) {

  private static final Pattern DESTINATION_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

  RelayRegistration {
    if (destinationId == null || destinationId.isBlank()) {
      throw new IllegalArgumentException("destinationId must be non-blank");
    }
    if (sourceTopic == null || sourceTopic.isBlank()) {
      throw new IllegalArgumentException("sourceTopic must be non-blank");
    }
    if (downstreamTopic == null || downstreamTopic.isBlank()) {
      throw new IllegalArgumentException("downstreamTopic must be non-blank");
    }
    if (consumer == null) {
      throw new IllegalArgumentException("consumer must not be null");
    }
    if (publisher == null) {
      throw new IllegalArgumentException("publisher must not be null");
    }
    if (!DESTINATION_ID_PATTERN.matcher(destinationId).matches()) {
      throw new IllegalArgumentException(
          "destinationId must match "
              + DESTINATION_ID_PATTERN.pattern()
              + ", got "
              + destinationId);
    }
  }
}
