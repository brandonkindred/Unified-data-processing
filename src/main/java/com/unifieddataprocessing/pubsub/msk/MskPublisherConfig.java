package com.unifieddataprocessing.pubsub.msk;

import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * Static factory that builds a {@link KafkaProducerConfig} pre-populated with the SASL properties
 * Amazon MSK requires for either AWS IAM (via {@code aws-msk-iam-auth}) or SASL/SCRAM-SHA-512.
 * MSK is wire-compatible with Apache Kafka, so callers continue to use the existing {@code
 * KafkaProducer} unchanged. Mirror of {@link
 * com.unifieddataprocessing.pubsub.msk.MskConsumerConfig} minus the consumer-only {@code groupId}.
 */
public final class MskPublisherConfig {

  private static final String IAM_LOGIN_MODULE = "software.amazon.msk.auth.iam.IAMLoginModule";
  private static final String IAM_CALLBACK_HANDLER =
      "software.amazon.msk.auth.iam.IAMClientCallbackHandler";
  private static final String SCRAM_LOGIN_MODULE =
      "org.apache.kafka.common.security.scram.ScramLoginModule";

  private MskPublisherConfig() {}

  /** Builds an IAM-auth Kafka producer config with no extra client properties. */
  public static KafkaProducerConfig iamAuth(String bootstrapServers, String region) {
    return iamAuth(bootstrapServers, region, Collections.emptyMap());
  }

  /**
   * Builds an IAM-auth Kafka producer config. Caller-supplied {@code extras} are merged in last,
   * so callers may override any of the SASL/security defaults set here.
   */
  public static KafkaProducerConfig iamAuth(
      String bootstrapServers, String region, Map<String, Object> extras) {
    Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    Objects.requireNonNull(region, "region");
    validateNoQuotes("region", region);

    String jaasConfig = IAM_LOGIN_MODULE + " required awsRegion=\"" + region + "\";";

    Map<String, Object> merged = new LinkedHashMap<>();
    merged.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    merged.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
    merged.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    merged.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, IAM_CALLBACK_HANDLER);
    if (extras != null) {
      merged.putAll(extras);
    }
    return new KafkaProducerConfig(bootstrapServers, merged);
  }

  /** Builds a SASL/SCRAM-SHA-512 Kafka producer config with no extra client properties. */
  public static KafkaProducerConfig saslScram(
      String bootstrapServers, String username, String password) {
    return saslScram(bootstrapServers, username, password, Collections.emptyMap());
  }

  /**
   * Builds a SASL/SCRAM-SHA-512 Kafka producer config. Caller-supplied {@code extras} are merged
   * in last, so callers may override any of the SASL/security defaults set here.
   */
  public static KafkaProducerConfig saslScram(
      String bootstrapServers, String username, String password, Map<String, Object> extras) {
    Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    Objects.requireNonNull(username, "username");
    Objects.requireNonNull(password, "password");
    validateNoQuotes("username", username);
    validateNoQuotes("password", password);

    String jaasConfig =
        SCRAM_LOGIN_MODULE
            + " required username=\""
            + username
            + "\" password=\""
            + password
            + "\";";

    Map<String, Object> merged = new LinkedHashMap<>();
    merged.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    merged.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
    merged.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    if (extras != null) {
      merged.putAll(extras);
    }
    return new KafkaProducerConfig(bootstrapServers, merged);
  }

  private static void validateNoQuotes(String name, String value) {
    if (value.indexOf('"') >= 0) {
      throw new IllegalArgumentException(name + " must not contain a double-quote character");
    }
  }
}
