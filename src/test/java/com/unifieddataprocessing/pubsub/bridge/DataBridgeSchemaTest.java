package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import com.unifieddataprocessing.pubsub.schema.InMemorySchemaRegistry;
import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.SchemaViolationPolicy;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import com.unifieddataprocessing.pubsub.schema.validators.MaxPayloadSizeSchemaValidator;
import com.unifieddataprocessing.pubsub.schema.validators.PermissiveSchemaValidator;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataBridgeSchemaTest {

  @Mock private PubSubConsumer consumerA;
  @Mock private PubSubPublisher mockPublisher;
  @Mock private AdminClient mockAdminClient;
  @Mock private CreateTopicsResult createTopicsResult;

  private Function<KafkaProducerConfig, PubSubPublisher> publisherFactory;
  private Function<Properties, AdminClient> adminFactory;
  private Function<Integer, ExecutorService> singleThreadFactory;
  private Sleeper noopSleeper;
  private InMemorySchemaRegistry registry;

  @BeforeEach
  void setUp() {
    publisherFactory = cfg -> mockPublisher;
    adminFactory = props -> mockAdminClient;
    singleThreadFactory = n -> Executors.newSingleThreadExecutor();
    noopSleeper = d -> {};
    registry = new InMemorySchemaRegistry();
  }

  private DataBridgeConfig configWith(SchemaValidator validator, SchemaViolationPolicy policy) {
    DataBridgeConfig.Builder b =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(500))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(3)
            .publishFailureCooldown(Duration.ofMillis(50))
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1);
    if (validator != null) {
      b.schemaRegistry(registry).schemaValidator(validator).schemaViolationPolicy(policy);
    }
    return b.build();
  }

  private DataBridge newBridge(DataBridgeConfig config) {
    return new DataBridge(
        config, publisherFactory, adminFactory, singleThreadFactory, noopSleeper);
  }

  private void stubKafkaHappyPath(String... topicNames) {
    Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
    for (String t : topicNames) {
      futures.put(t, KafkaFuture.completedFuture(null));
    }
    when(mockAdminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
    when(createTopicsResult.values()).thenReturn(futures);
    lenient()
        .when(mockPublisher.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  private static Message msg(String id, byte[] payload) {
    return new Message(id, "ignored-source-topic", payload, Map.of());
  }

  @Test
  void noSchemaRegistered_publishesAsUsual_noSchemaAttributes() throws Exception {
    stubKafkaHappyPath("src.chan");
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(msg("m-1", new byte[] {1, 2, 3})))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge =
        newBridge(configWith(new PermissiveSchemaValidator(), SchemaViolationPolicy.DROP));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    bridge.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher).publish(captor.capture());
    Message published = captor.getValue();
    assertEquals("src.chan", published.getTopic());
    assertEquals(null, published.getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_SUBJECT));
    assertEquals(null, published.getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_VERSION));
  }

  @Test
  void schemaRegistered_andPayloadValid_publishes_withVersionAttribute() throws Exception {
    stubKafkaHappyPath("src.chan");
    registry.register("src.chan", "JSON", "{\"type\":\"object\"}");

    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(msg("m-1", new byte[] {1, 2})))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge =
        newBridge(
            configWith(new MaxPayloadSizeSchemaValidator(10), SchemaViolationPolicy.DROP));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    bridge.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher).publish(captor.capture());
    Message published = captor.getValue();
    assertEquals("src.chan", published.getTopic());
    assertEquals(
        "src.chan", published.getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_SUBJECT));
    assertEquals("1", published.getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_VERSION));
  }

  @Test
  void schemaViolation_dropPolicy_acksAndSkipsPublish() throws Exception {
    stubKafkaHappyPath("src.chan");
    registry.register("src.chan", "JSON", "{}");

    Message tooBig = msg("big", new byte[100]);
    Message ok = msg("ok", new byte[] {1});
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(tooBig, ok))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(2);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge =
        newBridge(configWith(new MaxPayloadSizeSchemaValidator(10), SchemaViolationPolicy.DROP));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "both messages should ack");
    bridge.close();

    // Big message: validator fails, drop policy => acked, NOT published.
    // Small message: passes, gets published.
    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher, times(1)).publish(captor.capture());
    assertEquals("ok", captor.getValue().getId());
    verify(consumerA, times(2)).acknowledge(any(Message.class));
  }

  @Test
  void schemaViolation_failPolicy_breaksBatch_noAck_andRedelivers() throws Exception {
    stubKafkaHappyPath("src.chan");
    registry.register("src.chan", "JSON", "{}");

    Message tooBig = msg("big", new byte[100]);
    // Switching to a permissive validator on the second batch so the redelivery succeeds.
    AtomicInteger pollCount = new AtomicInteger();
    when(consumerA.poll(any(Duration.class)))
        .thenAnswer(
            inv -> {
              int n = pollCount.incrementAndGet();
              if (n == 1 || n == 2) {
                return List.of(tooBig);
              }
              return List.of();
            });

    // Validator: first call rejects (length > 10), subsequent calls accept anything.
    AtomicInteger validateCount = new AtomicInteger();
    SchemaValidator validator =
        (schema, payload) -> {
          int n = validateCount.incrementAndGet();
          if (n == 1) {
            return ValidationResult.fail("too big");
          }
          return ValidationResult.ok();
        };

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge(configWith(validator, SchemaViolationPolicy.FAIL));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS), "redelivered message should ack");
    bridge.close();

    // First batch: invalid → no publish, no ack. Second batch: valid → publish + ack.
    verify(mockPublisher, times(1)).publish(any(Message.class));
    verify(consumerA, times(1)).acknowledge(any(Message.class));
  }

  @Test
  void validatorThrows_treatedAsViolation_underDropPolicy_acksAndSkips() throws Exception {
    stubKafkaHappyPath("src.chan");
    registry.register("src.chan", "JSON", "{}");

    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(msg("m-1", new byte[] {1})))
        .thenReturn(List.of());

    SchemaValidator throwing =
        (schema, payload) -> {
          throw new RuntimeException("validator boom");
        };

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge(configWith(throwing, SchemaViolationPolicy.DROP));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "violating message should be dropped + acked");
    bridge.close();

    verify(mockPublisher, never()).publish(any(Message.class));
  }

  @Test
  void schemaUpdate_atRuntime_pickedUpOnNextBatch() throws Exception {
    stubKafkaHappyPath("src.chan");
    registry.register("src.chan", "JSON", "v1");

    CountDownLatch v1Ack = new CountDownLatch(1);
    AtomicInteger pollCount = new AtomicInteger();
    when(consumerA.poll(any(Duration.class)))
        .thenAnswer(
            inv -> {
              int n = pollCount.incrementAndGet();
              if (n == 1) {
                return List.of(msg("first", new byte[] {1}));
              }
              if (n == 2) {
                v1Ack.await(2, TimeUnit.SECONDS);
                registry.register("src.chan", "JSON", "v2");
                return List.of(msg("second", new byte[] {2}));
              }
              return List.of();
            });

    doAnswer(
            inv -> {
              v1Ack.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge =
        newBridge(configWith(new PermissiveSchemaValidator(), SchemaViolationPolicy.DROP));
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    // Wait for two publishes.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    while (System.nanoTime() < deadline) {
      try {
        verify(mockPublisher, atLeast(2)).publish(captor.capture());
        break;
      } catch (AssertionError ignored) {
        Thread.sleep(20);
      }
    }
    bridge.close();

    verify(mockPublisher, atLeast(2)).publish(captor.capture());
    Map<String, String> versionsById = new LinkedHashMap<>();
    for (Message m : captor.getAllValues()) {
      versionsById.put(
          m.getId(), m.getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_VERSION));
    }
    assertEquals("1", versionsById.get("first"));
    assertEquals("2", versionsById.get("second"));
  }

  @Test
  void perTopicSchema_isolatesUnknownTopic_fromValidated() throws Exception {
    // Only src.chanA has a schema; src.chanB is pass-through with no version stamps.
    // We exercise both registrations on the same bridge to confirm isolation.
    stubKafkaHappyPath("src.chanA", "src.chanB");
    registry.register("src.chanA", "JSON", "{}");

    PubSubConsumer consumerB = org.mockito.Mockito.mock(PubSubConsumer.class);
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(msg("a-1", new byte[] {1})))
        .thenReturn(List.of());
    when(consumerB.poll(any(Duration.class)))
        .thenReturn(List.of(msg("b-1", new byte[] {1})))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(2);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerB)
        .acknowledge(any(Message.class));

    singleThreadFactory = n -> Executors.newFixedThreadPool(n);

    DataBridge bridge =
        newBridge(configWith(new PermissiveSchemaValidator(), SchemaViolationPolicy.DROP));
    bridge.register("src", "chanA", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chanB", "topic-b", consumerB, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS));
    bridge.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher, times(2)).publish(captor.capture());
    Map<String, Message> byTopic = new LinkedHashMap<>();
    for (Message m : captor.getAllValues()) {
      byTopic.put(m.getTopic(), m);
    }
    assertEquals(
        "1",
        byTopic.get("src.chanA").getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_VERSION));
    assertEquals(
        null,
        byTopic.get("src.chanB").getAttributes().get(BridgeAttributes.BRIDGE_SCHEMA_VERSION));
  }

  @Test
  void registrySanity_latestReflectsLastRegistration() {
    Schema v1 = registry.register("src.chan", "JSON", "v1");
    Schema v2 = registry.register("src.chan", "JSON", "v2");
    assertEquals(1, v1.version());
    assertEquals(2, v2.version());
    assertEquals(v2, registry.latest("src.chan").orElseThrow());
  }
}
