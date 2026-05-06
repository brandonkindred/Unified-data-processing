package com.unifieddataprocessing.pubsub.pulsar;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Configuration for the Apache Pulsar-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubConsumer}. A single object carries both client-level
 * settings (broker URL, auth) and per-{@code Consumer} settings (subscription name/type) — this
 * matches the single-config style used by {@code KafkaConsumerConfig} and {@code
 * GcpPubSubConsumerConfig}.
 */
public final class PulsarConsumerConfig {

  /** Default {@code receive} batch budget per {@link
   * com.unifieddataprocessing.pubsub.PubSubConsumer#poll(java.time.Duration)} call. */
  public static final int DEFAULT_MAX_MESSAGES_PER_POLL = 100;

  /** Default Pulsar subscription type — safe choice for fan-out workloads. */
  public static final SubscriptionType DEFAULT_SUBSCRIPTION_TYPE = SubscriptionType.Shared;

  private final String serviceUrl;
  private final String subscriptionName;
  private final SubscriptionType subscriptionType;
  private final String authToken;
  private final int maxMessagesPerPoll;
  private final Duration operationTimeout;
  private final Map<String, Object> clientExtras;
  private final Map<String, Object> consumerExtras;

  /** Minimal configuration; all other fields take defaults. */
  public PulsarConsumerConfig(String serviceUrl, String subscriptionName) {
    this(
        serviceUrl,
        subscriptionName,
        DEFAULT_SUBSCRIPTION_TYPE,
        null,
        DEFAULT_MAX_MESSAGES_PER_POLL,
        null,
        null,
        null);
  }

  /**
   * Full configuration. {@code subscriptionType} defaults to {@link SubscriptionType#Shared} when
   * {@code null}. {@code authToken} (JWT bearer), {@code operationTimeout}, and the two extras maps
   * may all be {@code null} (interpreted as "use Pulsar client defaults" / empty).
   *
   * @throws IllegalArgumentException if {@code maxMessagesPerPoll <= 0}.
   */
  public PulsarConsumerConfig(
      String serviceUrl,
      String subscriptionName,
      SubscriptionType subscriptionType,
      String authToken,
      int maxMessagesPerPoll,
      Duration operationTimeout,
      Map<String, Object> clientExtras,
      Map<String, Object> consumerExtras) {
    this.serviceUrl = Objects.requireNonNull(serviceUrl, "serviceUrl");
    this.subscriptionName = Objects.requireNonNull(subscriptionName, "subscriptionName");
    this.subscriptionType = subscriptionType == null ? DEFAULT_SUBSCRIPTION_TYPE : subscriptionType;
    this.authToken = authToken;
    if (maxMessagesPerPoll <= 0) {
      throw new IllegalArgumentException("maxMessagesPerPoll must be > 0");
    }
    this.maxMessagesPerPoll = maxMessagesPerPoll;
    this.operationTimeout = operationTimeout;
    this.clientExtras =
        clientExtras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(clientExtras));
    this.consumerExtras =
        consumerExtras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(consumerExtras));
  }

  public String getServiceUrl() {
    return serviceUrl;
  }

  public String getSubscriptionName() {
    return subscriptionName;
  }

  public SubscriptionType getSubscriptionType() {
    return subscriptionType;
  }

  public String getAuthToken() {
    return authToken;
  }

  public int getMaxMessagesPerPoll() {
    return maxMessagesPerPoll;
  }

  public Duration getOperationTimeout() {
    return operationTimeout;
  }

  /** Applies this config to a {@link ClientBuilder} and returns it for chaining. */
  public ClientBuilder applyToClientBuilder(ClientBuilder builder) {
    Objects.requireNonNull(builder, "builder");
    builder.serviceUrl(serviceUrl);
    if (authToken != null) {
      builder.authentication(AuthenticationFactory.token(authToken));
    }
    if (operationTimeout != null) {
      builder.operationTimeout((int) operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
    if (!clientExtras.isEmpty()) {
      builder.loadConf(clientExtras);
    }
    return builder;
  }

  /**
   * Applies this config plus the given {@code topics} to a {@link ConsumerBuilder} and returns it
   * for chaining.
   */
  public ConsumerBuilder<byte[]> applyToConsumerBuilder(
      ConsumerBuilder<byte[]> builder, Collection<String> topics) {
    Objects.requireNonNull(builder, "builder");
    Objects.requireNonNull(topics, "topics");
    builder.subscriptionName(subscriptionName);
    builder.subscriptionType(subscriptionType);
    builder.topics(new java.util.ArrayList<>(topics));
    if (!consumerExtras.isEmpty()) {
      builder.loadConf(consumerExtras);
    }
    return builder;
  }
}
