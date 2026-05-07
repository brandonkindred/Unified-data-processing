package com.unifieddataprocessing.pubsub.pulsar;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;

/**
 * Configuration for the Apache Pulsar-backed {@link
 * com.unifieddataprocessing.pubsub.PubSubPublisher}. Mirrors {@link PulsarConsumerConfig} minus the
 * subscription-side fields ({@code subscriptionName}, {@code subscriptionType}, {@code
 * maxMessagesPerPoll}). Topic comes from each {@link
 * com.unifieddataprocessing.pubsub.Message#getTopic()}, never from this config.
 */
public final class PulsarPublisherConfig {

  private final String serviceUrl;
  private final String authToken;
  private final Duration operationTimeout;
  private final Map<String, Object> clientExtras;
  private final Map<String, Object> producerExtras;

  /** Minimal configuration; all other fields take Pulsar client defaults. */
  public PulsarPublisherConfig(String serviceUrl) {
    this(serviceUrl, null, null, null, null);
  }

  /**
   * Full configuration. {@code authToken} (JWT bearer), {@code operationTimeout}, and the two
   * extras maps may all be {@code null} (Pulsar defaults / empty).
   */
  public PulsarPublisherConfig(
      String serviceUrl,
      String authToken,
      Duration operationTimeout,
      Map<String, Object> clientExtras,
      Map<String, Object> producerExtras) {
    this.serviceUrl = Objects.requireNonNull(serviceUrl, "serviceUrl");
    this.authToken = authToken;
    this.operationTimeout = operationTimeout;
    this.clientExtras =
        clientExtras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(clientExtras));
    this.producerExtras =
        producerExtras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(producerExtras));
  }

  public String getServiceUrl() {
    return serviceUrl;
  }

  public String getAuthToken() {
    return authToken;
  }

  public Duration getOperationTimeout() {
    return operationTimeout;
  }

  public Map<String, Object> getClientExtras() {
    return clientExtras;
  }

  public Map<String, Object> getProducerExtras() {
    return producerExtras;
  }

  /** Applies this config to a {@link ClientBuilder} and returns it for chaining. */
  public ClientBuilder applyToClientBuilder(ClientBuilder builder) {
    Objects.requireNonNull(builder, "builder");
    // Apply caller-supplied extras first so wrapper-controlled fields below remain
    // authoritative — otherwise extras like `serviceUrl` could override the configured
    // broker. Mirrors PulsarConsumerConfig.applyToClientBuilder ordering.
    if (!clientExtras.isEmpty()) {
      builder.loadConf(clientExtras);
    }
    builder.serviceUrl(serviceUrl);
    if (authToken != null) {
      builder.authentication(AuthenticationFactory.token(authToken));
    }
    if (operationTimeout != null) {
      builder.operationTimeout((int) operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
    return builder;
  }

  /**
   * Applies this config plus the given {@code topic} to a {@link ProducerBuilder} and returns it
   * for chaining.
   */
  public ProducerBuilder<byte[]> applyToProducerBuilder(
      ProducerBuilder<byte[]> builder, String topic) {
    Objects.requireNonNull(builder, "builder");
    Objects.requireNonNull(topic, "topic");
    // Apply caller-supplied extras first so wrapper-controlled fields (topic) cannot be
    // overridden by extras like `topicName`. Mirrors PulsarConsumerConfig.applyToConsumerBuilder
    // ordering. Callers can still tune batchingMax*, sendTimeoutMs, compressionType, etc.
    if (!producerExtras.isEmpty()) {
      builder.loadConf(producerExtras);
    }
    builder.topic(topic);
    return builder;
  }
}
