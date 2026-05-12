package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataBridgeTest {

  @Mock private PubSubConsumer consumerA;
  @Mock private PubSubConsumer consumerB;
  @Mock private PubSubPublisher mockPublisher;
  @Mock private AdminClient mockAdminClient;
  @Mock private CreateTopicsResult createTopicsResult;

  private Function<KafkaProducerConfig, PubSubPublisher> publisherFactory;
  private Function<Properties, AdminClient> adminFactory;
  private Function<Integer, ExecutorService> singleThreadFactory;
  private Sleeper noopSleeper;
  private DataBridgeConfig config;

  @BeforeEach
  void setUp() {
    publisherFactory = cfg -> mockPublisher;
    adminFactory = props -> mockAdminClient;
    singleThreadFactory = n -> Executors.newSingleThreadExecutor();
    noopSleeper = d -> {};
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(500))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
  }

  private DataBridge newBridge() {
    return new DataBridge(
        config, publisherFactory, adminFactory, singleThreadFactory, noopSleeper);
  }

  /** Stubs the Kafka admin + publisher mocks for tests that exercise the happy path. */
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

  private static Message msg(String id, String topic, Map<String, String> attrs) {
    return new Message(id, topic, new byte[] {1, 2, 3}, attrs);
  }

  @Test
  void happyPath_provisionsConnectsSubscribesPublishesAcksInOrder() throws Exception {
    stubKafkaHappyPath("src.chan");
    Message source = msg("m-1", "ignored-source-topic", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(source))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "consumerA should have acked the message");
    bridge.close();

    InOrder order = inOrder(mockAdminClient, mockPublisher, consumerA);
    order.verify(mockAdminClient).createTopics(anyCollection());
    order.verify(mockPublisher).connect();
    order.verify(consumerA).connect();
    order.verify(consumerA).subscribe("src-topic");
    order.verify(consumerA).poll(any(Duration.class));
    order.verify(mockPublisher).publish(any(Message.class));
    order.verify(consumerA).acknowledge(any(Message.class));
  }

  @Test
  void happyPath_rewrittenMessageHasTargetTopicAndBridgeAttrs() throws Exception {
    stubKafkaHappyPath("src.chan");
    Map<String, String> callerAttrs = new LinkedHashMap<>();
    callerAttrs.put(BridgeAttributes.BRIDGE_SOURCE_ID, "wrong");
    callerAttrs.put("kafkaKey", "k-1");
    Message source = msg("m-1", "ignored-source-topic", callerAttrs);
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(source))
        .thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    bridge.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher).publish(captor.capture());
    Message published = captor.getValue();
    assertEquals("src.chan", published.getTopic());
    assertEquals("src", published.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_ID));
    assertEquals("src-topic", published.getAttributes().get(BridgeAttributes.BRIDGE_SOURCE_TOPIC));
    assertEquals("chan", published.getAttributes().get(BridgeAttributes.BRIDGE_CHANNEL));
    assertEquals("k-1", published.getAttributes().get("kafkaKey"));
    assertEquals("m-1", published.getId());
  }

  @Test
  void twoSources_eachPublishedAndAcked() throws Exception {
    stubKafkaHappyPath("src.chanA", "src.chanB");

    Message messageA = msg("m-A", "topic-a", Map.of());
    Message messageB = msg("m-B", "topic-b", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(messageA)).thenReturn(List.of());
    when(consumerB.poll(any(Duration.class))).thenReturn(List.of(messageB)).thenReturn(List.of());

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

    // Override the test default (single-thread) so two poll loops can run concurrently.
    singleThreadFactory = n -> Executors.newFixedThreadPool(n);

    DataBridge bridge = newBridge();
    bridge.register("src", "chanA", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chanB", "topic-b", consumerB, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "both consumers should have acked");
    bridge.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(mockPublisher, times(2)).publish(captor.capture());
    Set<String> publishedTopics =
        captor.getAllValues().stream().map(Message::getTopic).collect(Collectors.toSet());
    assertEquals(Set.of("src.chanA", "src.chanB"), publishedTopics);
  }

  @Test
  void register_afterStart_throwsIllegalStateException() {
    stubKafkaHappyPath("src.chan");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(bridge.isRunning());

    assertThrows(
        IllegalStateException.class,
        () -> bridge.register("src2", "chan2", "t2", consumerB, ChannelOptions.defaults()));

    bridge.close();
  }

  @Test
  void register_duplicateSourceIdChannelPair_throwsIllegalArgumentException() {
    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-1", consumerA, ChannelOptions.defaults());
    assertThrows(
        IllegalArgumentException.class,
        () -> bridge.register("src", "chan", "topic-2", consumerB, ChannelOptions.defaults()));
  }

  @Test
  void register_duplicateConsumerInstance_throwsIllegalArgumentException() {
    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-1", consumerA, ChannelOptions.defaults());
    assertThrows(
        IllegalArgumentException.class,
        () -> bridge.register("src", "chan2", "topic-2", consumerA, ChannelOptions.defaults()));
  }

  @Test
  void register_invalidSourceId_throwsIllegalArgumentException() {
    DataBridge bridge = newBridge();
    assertThrows(
        IllegalArgumentException.class,
        () -> bridge.register("a.b", "chan", "topic", consumerA, ChannelOptions.defaults()));
  }

  @Test
  void register_invalidChannel_throwsIllegalArgumentException() {
    DataBridge bridge = newBridge();
    assertThrows(
        IllegalArgumentException.class,
        () -> bridge.register("src", "a/b", "topic", consumerA, ChannelOptions.defaults()));
  }

  @Test
  void register_combinedLengthOver249_throwsIllegalArgumentException() {
    DataBridge bridge = newBridge();
    String sourceId = "s".repeat(124);
    String channel = "c".repeat(125);
    assertEquals(250, sourceId.length() + 1 + channel.length());
    assertThrows(
        IllegalArgumentException.class,
        () -> bridge.register(sourceId, channel, "topic", consumerA, ChannelOptions.defaults()));
  }

  @Test
  void start_twice_throwsIllegalStateException() {
    stubKafkaHappyPath("src.chan");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertThrows(IllegalStateException.class, bridge::start);
    bridge.close();
  }

  @Test
  void start_withNoRegistrations_throwsIllegalStateException() {
    DataBridge bridge = newBridge();
    assertThrows(IllegalStateException.class, bridge::start);
  }

  @Test
  void start_provisioningFailure_cleansUp_publisherNotConnected_stateClosed_closeIsNoOp() {
    KafkaFutureImpl<Void> authFuture = new KafkaFutureImpl<>();
    authFuture.completeExceptionally(new TopicAuthorizationException("denied"));
    Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
    futures.put("src.chan", authFuture);
    when(mockAdminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
    when(createTopicsResult.values()).thenReturn(futures);

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());

    assertThrows(RuntimeException.class, bridge::start);
    assertFalse(bridge.isRunning());

    bridge.close();

    verify(mockPublisher, never()).connect();
    verify(consumerA, never()).connect();
    verify(mockPublisher, never()).flush();
    verify(mockPublisher, never()).close();
  }

  @Test
  void start_subscribeFailure_cleansUpConnectedConsumers_andPublisher_stateClosed() {
    stubKafkaHappyPath("src.chan", "src.chan2");
    doThrow(new RuntimeException("subscribe boom")).when(consumerB).subscribe("topic-b");

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chan2", "topic-b", consumerB, ChannelOptions.defaults());

    assertThrows(RuntimeException.class, bridge::start);
    assertFalse(bridge.isRunning());

    bridge.close();

    verify(consumerA).connect();
    verify(consumerB).connect();
    verify(consumerA, times(1)).close();
    verify(consumerB, times(1)).close();
    verify(mockPublisher, times(1)).close();
  }

  @Test
  void close_flushesBeforeClosingPublisher() {
    stubKafkaHappyPath("src.chan");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    bridge.close();

    InOrder order = inOrder(mockPublisher);
    order.verify(mockPublisher).flush();
    order.verify(mockPublisher).close();
  }

  @Test
  void close_tolerates_consumerThatThrowsOnClose() throws Exception {
    stubKafkaHappyPath("src.chan", "src.chan2");
    doThrow(new RuntimeException("close boom")).when(consumerA).close();
    Message m = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m))
        .thenReturn(List.of());
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chan2", "topic-b", consumerB, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    bridge.close();

    verify(consumerA).close();
    verify(consumerB).close();
    verify(mockPublisher).close();
  }

  @Test
  void close_calledTwice_isNoOp() {
    stubKafkaHappyPath("src.chan");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "src-topic", consumerA, ChannelOptions.defaults());
    bridge.start();
    bridge.close();
    bridge.close();

    verify(mockPublisher, times(1)).close();
    verify(consumerA, times(1)).close();
  }
}
