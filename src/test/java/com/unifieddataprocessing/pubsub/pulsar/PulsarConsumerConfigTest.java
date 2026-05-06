package com.unifieddataprocessing.pubsub.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class PulsarConsumerConfigTest {

  @Test
  @SuppressWarnings("unchecked")
  void applyToConsumerBuilder_appliesExtrasBeforeWrapperControlledFields() {
    // Pulsar's ConsumerBuilder.loadConf(Map) accepts keys like `subscriptionName`,
    // `topicNames`, and `subscriptionType`. If extras are applied AFTER the wrapper-set
    // fields, a caller-supplied `subscriptionName` in extras would silently override
    // subscribe(String). Wrapper-controlled fields must always win.
    ConsumerBuilder<byte[]> builder = mock(ConsumerBuilder.class);
    Map<String, Object> extras = new LinkedHashMap<>();
    extras.put("subscriptionName", "extras-tries-to-override");
    extras.put("ackTimeoutMillis", 5000);
    PulsarConsumerConfig config =
        new PulsarConsumerConfig(
            "pulsar://broker:6650",
            "wrapper-subscription",
            SubscriptionType.Failover,
            null,
            100,
            null,
            null,
            extras);

    config.applyToConsumerBuilder(builder, List.of("topic-x"));

    InOrder order = inOrder(builder);
    order.verify(builder).loadConf(extras);
    order.verify(builder).subscriptionName("wrapper-subscription");
    order.verify(builder).subscriptionType(SubscriptionType.Failover);
    order.verify(builder).topics(any(List.class));
  }

  @Test
  void applyToClientBuilder_appliesExtrasBeforeWrapperControlledFields() {
    ClientBuilder builder = mock(ClientBuilder.class);
    Map<String, Object> extras = Map.of("serviceUrl", "pulsar://wrong:6650");
    PulsarConsumerConfig config =
        new PulsarConsumerConfig(
            "pulsar://right:6650",
            "sub",
            null,
            "the-token",
            100,
            Duration.ofSeconds(30),
            extras,
            null);

    config.applyToClientBuilder(builder);

    InOrder order = inOrder(builder);
    order.verify(builder).loadConf(extras);
    order.verify(builder).serviceUrl("pulsar://right:6650");
    order.verify(builder).authentication(any(Authentication.class));
    order.verify(builder).operationTimeout(eq(30000), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  @SuppressWarnings("unchecked")
  void applyToConsumerBuilder_skipsLoadConfWhenExtrasEmpty() {
    ConsumerBuilder<byte[]> builder = mock(ConsumerBuilder.class);
    PulsarConsumerConfig config = new PulsarConsumerConfig("pulsar://broker:6650", "sub");

    config.applyToConsumerBuilder(builder, List.of("topic-x"));

    verify(builder, never()).loadConf(any());
  }

  @Test
  void applyToClientBuilder_skipsLoadConfWhenExtrasEmpty() {
    ClientBuilder builder = mock(ClientBuilder.class);
    PulsarConsumerConfig config = new PulsarConsumerConfig("pulsar://broker:6650", "sub");

    config.applyToClientBuilder(builder);

    verify(builder, never()).loadConf(any());
  }

  @Test
  void constructor_rejectsNonPositiveMaxMessagesPerPoll() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PulsarConsumerConfig(
                "pulsar://broker:6650", "sub", null, null, 0, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PulsarConsumerConfig(
                "pulsar://broker:6650", "sub", null, null, -5, null, null, null));
  }

  @Test
  void constructor_rejectsNullRequiredFields() {
    assertThrows(NullPointerException.class, () -> new PulsarConsumerConfig(null, "sub"));
    assertThrows(
        NullPointerException.class, () -> new PulsarConsumerConfig("pulsar://x:6650", null));
  }

  @Test
  void constructor_appliesDefaultsForNullSubscriptionType() {
    PulsarConsumerConfig config =
        new PulsarConsumerConfig(
            "pulsar://broker:6650", "sub", null, null, 100, null, null, null);
    assertEquals(
        PulsarConsumerConfig.DEFAULT_SUBSCRIPTION_TYPE, config.getSubscriptionType());
  }
}
