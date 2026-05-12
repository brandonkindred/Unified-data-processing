package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.PubSubConsumer;
import java.util.regex.Pattern;

/**
 * One registered source-to-channel binding inside a {@code DataBridge}.
 *
 * <p>Validation runs in the compact constructor so a {@code Registration} can never exist in an
 * invalid state. The caller pre-computes {@code targetTopic} as {@code sourceId + "." + channel};
 * the constructor asserts the equality so the dotted prefix carved out of the topic name is
 * unambiguous.
 */
record Registration(
    String sourceId,
    String channel,
    String sourceTopic,
    String targetTopic,
    PubSubConsumer consumer,
    ChannelOptions options) {

  private static final Pattern SOURCE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");
  private static final Pattern CHANNEL_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
  private static final int MAX_TOPIC_NAME_LENGTH = 249;

  Registration {
    if (sourceId == null || sourceId.isBlank()) {
      throw new IllegalArgumentException("sourceId must be non-blank");
    }
    if (channel == null || channel.isBlank()) {
      throw new IllegalArgumentException("channel must be non-blank");
    }
    if (sourceTopic == null || sourceTopic.isBlank()) {
      throw new IllegalArgumentException("sourceTopic must be non-blank");
    }
    if (targetTopic == null || targetTopic.isBlank()) {
      throw new IllegalArgumentException("targetTopic must be non-blank");
    }
    if (consumer == null) {
      throw new IllegalArgumentException("consumer must not be null");
    }
    if (options == null) {
      throw new IllegalArgumentException("options must not be null");
    }
    if (!SOURCE_ID_PATTERN.matcher(sourceId).matches()) {
      throw new IllegalArgumentException(
          "sourceId must match " + SOURCE_ID_PATTERN.pattern() + ", got " + sourceId);
    }
    if (!CHANNEL_PATTERN.matcher(channel).matches()) {
      throw new IllegalArgumentException(
          "channel must match " + CHANNEL_PATTERN.pattern() + ", got " + channel);
    }
    if (sourceId.length() + 1 + channel.length() > MAX_TOPIC_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "sourceId + \".\" + channel must be at most "
              + MAX_TOPIC_NAME_LENGTH
              + " characters, got "
              + (sourceId.length() + 1 + channel.length()));
    }
    String expectedTargetTopic = sourceId + "." + channel;
    if (!expectedTargetTopic.equals(targetTopic)) {
      throw new IllegalArgumentException(
          "targetTopic must equal sourceId + \".\" + channel ("
              + expectedTargetTopic
              + "), got "
              + targetTopic);
    }
  }
}
