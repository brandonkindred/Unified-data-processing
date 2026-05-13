package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.atLeast;
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
import com.unifieddataprocessing.pubsub.PublishResult;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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

  private DataBridge newBridge(Sleeper sleeper) {
    return new DataBridge(config, publisherFactory, adminFactory, singleThreadFactory, sleeper);
  }

  /** Records every {@link Sleeper#sleep(Duration)} invocation; thread-safe for worker threads. */
  static final class CountingSleeper implements Sleeper {
    final List<Duration> sleeps = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void sleep(Duration d) {
      sleeps.add(d);
    }
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
  void pollThrowsThenRecovers() throws Exception {
    stubKafkaHappyPath("src.chan");
    Message source = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenThrow(new RuntimeException("transient poll error"))
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

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "consumerA should ack after poll recovers");
    bridge.close();

    verify(mockPublisher, times(1)).publish(any(Message.class));
    verify(consumerA, times(1)).acknowledge(any(Message.class));
    assertEquals(List.of(config.pollBackoff()), sleeper.sleeps);
  }

  @Test
  void publishTimesOut_skipsAck_breaksBatch() throws Exception {
    // Shorten publishTimeout so the timed-out get() returns quickly.
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(100))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chan");

    Message m1 = msg("m-1", "topic-a", Map.of());
    Message m2 = msg("m-2", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of());

    // First publish never completes (triggers TimeoutException); subsequent calls succeed.
    CompletableFuture<PublishResult> neverCompletes = new CompletableFuture<>();
    when(mockPublisher.publish(any(Message.class)))
        .thenReturn(neverCompletes)
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(2);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS), "both messages should ack after redelivery");
    bridge.close();

    // Batch 1: publish(m1) times out, break batch, neither m1 nor m2 acked, m2 not published.
    // Batch 2: publish(m1) + publish(m2) both succeed, both acked.
    verify(mockPublisher, times(3)).publish(any(Message.class));
    verify(consumerA, times(2)).acknowledge(any(Message.class));
  }

  @Test
  void publishFails_skipsAck_breaksBatch() throws Exception {
    stubKafkaHappyPath("src.chan");

    Message m1 = msg("m-1", "topic-a", Map.of());
    Message m2 = msg("m-2", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of());

    CompletableFuture<PublishResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("publish-fail"));
    when(mockPublisher.publish(any(Message.class)))
        .thenReturn(failed)
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(2);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataBridge bridge = newBridge();
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS), "both messages should ack after redelivery");
    bridge.close();

    verify(mockPublisher, times(3)).publish(any(Message.class));
    verify(consumerA, times(2)).acknowledge(any(Message.class));
  }

  @Test
  void failingSourceDoesNotBlockHealthySource() throws Exception {
    stubKafkaHappyPath("src.chanA", "src.chanB");

    AtomicInteger counter = new AtomicInteger();
    when(consumerA.poll(any(Duration.class)))
        .thenAnswer(
            inv -> List.of(msg("a-" + counter.incrementAndGet(), "topic-a", Map.of())));
    when(consumerB.poll(any(Duration.class)))
        .thenAnswer(
            inv -> {
              throw new RuntimeException("source-b broken");
            });

    CountDownLatch ackLatch = new CountDownLatch(3);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    // Real two-thread pool so the failing source can't starve the healthy one.
    singleThreadFactory = n -> Executors.newFixedThreadPool(n);

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chanA", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chanB", "topic-b", consumerB, ChannelOptions.defaults());
    bridge.start();
    assertTrue(ackLatch.await(5, TimeUnit.SECONDS), "source A should ack >= 3 times");
    // Wait for source B to have run its catch+backoff at least once — under tight scheduling A
    // can finish 3 acks before B's worker is even scheduled.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (sleeper.sleeps.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }

    long t0 = System.nanoTime();
    bridge.close();
    long elapsedNs = System.nanoTime() - t0;

    verify(consumerA, atLeast(3)).acknowledge(any(Message.class));
    assertTrue(
        sleeper.sleeps.size() >= 1,
        "source B's poll failures should have triggered at least one backoff sleep");

    // shutdownTimeout + closeForceTimeout is the hard upper bound for executor termination;
    // add a small scheduling grace for JVM cold-start so the test is stable on slow CI runners.
    Duration budget =
        config.shutdownTimeout().plus(config.closeForceTimeout()).plus(Duration.ofMillis(500));
    assertTrue(
        elapsedNs <= budget.toNanos(),
        "close() took " + Duration.ofNanos(elapsedNs) + " but budget is " + budget);
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

  @Test
  void perChannelOptionsHonored() {
    stubKafkaHappyPath("srcA.chA", "srcB.chB");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register("srcA", "chA", "src-topic-a", consumerA, ChannelOptions.defaults());
    bridge.register(
        "srcB",
        "chB",
        "src-topic-b",
        consumerB,
        ChannelOptions.builder()
            .partitions(6)
            .replicationFactor((short) 3)
            .topicConfig("retention.ms", "604800000")
            .build());
    bridge.start();
    bridge.close();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockAdminClient).createTopics(captor.capture());
    Map<String, NewTopic> byName =
        captor.getValue().stream().collect(Collectors.toMap(NewTopic::name, t -> t));

    NewTopic ntA = byName.get("srcA.chA");
    assertEquals(1, ntA.numPartitions());
    assertEquals((short) 1, ntA.replicationFactor());
    assertTrue(
        ntA.configs() == null || ntA.configs().isEmpty(),
        "defaults() should pass through an empty topicConfigs map");

    NewTopic ntB = byName.get("srcB.chB");
    assertEquals(6, ntB.numPartitions());
    assertEquals((short) 3, ntB.replicationFactor());
    assertEquals(Map.of("retention.ms", "604800000"), ntB.configs());
  }

  @Test
  void partialOverride_fallsBackToDefaultsForUnsetField() {
    // Use a non-default replicationFactor so the fallback path is observable.
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(500))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 2)
            .build();
    stubKafkaHappyPath("src.chan");
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataBridge bridge = newBridge();
    bridge.register(
        "src", "chan", "src-topic", consumerA, ChannelOptions.builder().partitions(8).build());
    bridge.start();
    bridge.close();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockAdminClient).createTopics(captor.capture());
    NewTopic nt = captor.getValue().iterator().next();
    assertEquals("src.chan", nt.name());
    assertEquals(8, nt.numPartitions());
    assertEquals((short) 2, nt.replicationFactor());
    assertTrue(
        nt.configs() == null || nt.configs().isEmpty(),
        "unset topicConfigs should remain empty");
  }

  @Test
  void publishFailures_engageCircuitBreaker_afterThreshold() throws Exception {
    // Threshold 3: after 3 consecutive publish failures the worker should sleep
    // for publishFailureCooldown exactly once. Tight publishTimeout so the
    // timed-out get() returns fast.
    Duration cooldown = Duration.ofMillis(123);
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(20))
            .publishTimeout(Duration.ofMillis(50))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(3)
            .publishFailureCooldown(cooldown)
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chan");

    Message m = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(m));

    // Every publish fails with ExecutionException.
    CompletableFuture<PublishResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("downstream-down"));
    when(mockPublisher.publish(any(Message.class))).thenReturn(failed);

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();

    // Wait until the breaker has tripped at least once (a cooldown-duration
    // sleep appears) and then close.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      synchronized (sleeper.sleeps) {
        if (sleeper.sleeps.contains(cooldown)) {
          break;
        }
      }
      Thread.sleep(10);
    }
    bridge.close();

    long cooldownSleeps;
    synchronized (sleeper.sleeps) {
      cooldownSleeps = sleeper.sleeps.stream().filter(d -> d.equals(cooldown)).count();
    }
    assertTrue(
        cooldownSleeps >= 1,
        "expected at least one cooldown-duration sleep, got " + cooldownSleeps);
    verify(consumerA, never()).acknowledge(any(Message.class));
  }

  @Test
  void circuitBreaker_probeState_tripsAgainAfterSingleFailure() throws Exception {
    // After the first cooldown, the worker enters a probe state: one publish
    // failure (not `threshold` of them) trips the breaker again. With
    // threshold=3 and an always-failing publisher, we should observe a
    // cooldown approximately every 1 poll after the first trip, not every 3.
    Duration cooldown = Duration.ofMillis(50);
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(20))
            .publishTimeout(Duration.ofMillis(50))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(3)
            .publishFailureCooldown(cooldown)
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chan");

    Message m = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(m));

    AtomicInteger publishCount = new AtomicInteger();
    CompletableFuture<PublishResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("publish-fail"));
    when(mockPublisher.publish(any(Message.class)))
        .thenAnswer(
            inv -> {
              publishCount.incrementAndGet();
              return failed;
            });

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();

    // Wait until at least 3 cooldown sleeps have happened.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      synchronized (sleeper.sleeps) {
        long count = sleeper.sleeps.stream().filter(d -> d.equals(cooldown)).count();
        if (count >= 3) {
          break;
        }
      }
      Thread.sleep(10);
    }
    bridge.close();

    long cooldownSleeps;
    int publishes;
    synchronized (sleeper.sleeps) {
      cooldownSleeps = sleeper.sleeps.stream().filter(d -> d.equals(cooldown)).count();
    }
    publishes = publishCount.get();
    assertTrue(
        cooldownSleeps >= 3,
        "expected >= 3 cooldowns under probe-state behavior; got " + cooldownSleeps);
    // Under the old reset-to-zero behavior, observing 3 cooldowns would have
    // required ~3 * threshold = 9 publishes. Under probe state, the first
    // cooldown takes `threshold` publishes and each subsequent cooldown takes
    // 1, so 3 cooldowns => <= threshold + (cooldownSleeps - 1) + a small
    // slack for the trailing publish before close.
    assertTrue(
        publishes <= config.publishFailureThreshold() + cooldownSleeps + 1,
        "probe state should keep publish count tight; cooldowns="
            + cooldownSleeps
            + " publishes="
            + publishes);
  }

  @Test
  void circuitBreaker_resets_afterSuccessfulPublish() throws Exception {
    // Threshold 3 with a cyclic fail/fail/ok publish pattern. Counter climbs
    // to 2 then resets on the ok, so the breaker must never trip.
    Duration cooldown = Duration.ofMillis(777);
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(20))
            .publishTimeout(Duration.ofMillis(50))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(3)
            .publishFailureCooldown(cooldown)
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chan");

    Message m = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(m));

    AtomicInteger publishCount = new AtomicInteger();
    when(mockPublisher.publish(any(Message.class)))
        .thenAnswer(
            inv -> {
              int n = publishCount.incrementAndGet();
              if (n % 3 == 0) {
                return CompletableFuture.completedFuture(null);
              }
              CompletableFuture<PublishResult> failed = new CompletableFuture<>();
              failed.completeExceptionally(new RuntimeException("publish-fail"));
              return failed;
            });

    CountDownLatch ackLatch = new CountDownLatch(3);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();
    assertTrue(
        ackLatch.await(5, TimeUnit.SECONDS),
        "every third publish succeeds; counter resets so the worker keeps acking");
    bridge.close();

    long cooldownSleeps;
    synchronized (sleeper.sleeps) {
      cooldownSleeps = sleeper.sleeps.stream().filter(d -> d.equals(cooldown)).count();
    }
    assertEquals(
        0,
        cooldownSleeps,
        "successful publish should have reset the failure counter; cooldown should not trip");
  }

  @Test
  void circuitBreakerCooldown_interruptible_onShutdown() throws Exception {
    // Long cooldown — close() must interrupt the cooldown sleep and bring the
    // worker down inside the configured shutdown budget instead of waiting it
    // out. Threshold 1 to trip on the first failure.
    Duration cooldown = Duration.ofSeconds(60);
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(20))
            .publishTimeout(Duration.ofMillis(50))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(1)
            .publishFailureCooldown(cooldown)
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chan");

    Message m = msg("m-1", "topic-a", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(m));

    CountDownLatch publishLatch = new CountDownLatch(1);
    CompletableFuture<PublishResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("publish-fail"));
    when(mockPublisher.publish(any(Message.class)))
        .thenAnswer(
            inv -> {
              publishLatch.countDown();
              return failed;
            });

    // A sleeper that blocks like Thread.sleep but reacts to interrupt.
    Sleeper realSleeper = d -> TimeUnit.NANOSECONDS.sleep(d.toNanos());

    DataBridge bridge = newBridge(realSleeper);
    bridge.register("src", "chan", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.start();
    // Wait until at least one publish has occurred (= worker is in cooldown).
    assertTrue(
        publishLatch.await(2, TimeUnit.SECONDS), "worker should publish-fail and enter cooldown");

    long t0 = System.nanoTime();
    bridge.close();
    long elapsedNs = System.nanoTime() - t0;

    Duration budget =
        config.shutdownTimeout().plus(config.closeForceTimeout()).plus(Duration.ofMillis(500));
    assertTrue(
        elapsedNs <= budget.toNanos(),
        "close() took "
            + Duration.ofNanos(elapsedNs)
            + " but budget is "
            + budget
            + " (cooldown sleep should be interrupted)");
  }

  @Test
  void circuitBreaker_perRegistration_doesNotPauseHealthyOne() throws Exception {
    // Two registrations: A's publishes always succeed, B's always fail. B's
    // breaker engages but A keeps making progress on its own worker. Threshold 1
    // so B trips quickly; cooldown short so the test stays fast.
    Duration cooldown = Duration.ofMillis(50);
    config =
        DataBridgeConfig.builder()
            .producerConfig(new KafkaProducerConfig("broker:9092"))
            .pollTimeout(Duration.ofMillis(20))
            .publishTimeout(Duration.ofMillis(50))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .publishFailureThreshold(1)
            .publishFailureCooldown(cooldown)
            .defaultPartitions(1)
            .defaultReplicationFactor((short) 1)
            .build();
    stubKafkaHappyPath("src.chanA", "src.chanB");

    AtomicInteger counter = new AtomicInteger();
    when(consumerA.poll(any(Duration.class)))
        .thenAnswer(
            inv -> List.of(msg("a-" + counter.incrementAndGet(), "topic-a", Map.of())));
    when(consumerB.poll(any(Duration.class)))
        .thenReturn(List.of(msg("b-1", "topic-b", Map.of())));

    CompletableFuture<PublishResult> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("b-down"));
    // Publisher.publish: any message destined to src.chanA succeeds; src.chanB fails.
    when(mockPublisher.publish(any(Message.class)))
        .thenAnswer(
            inv -> {
              Message published = inv.getArgument(0);
              if ("src.chanB".equals(published.getTopic())) {
                return failedFuture;
              }
              return CompletableFuture.completedFuture(null);
            });

    CountDownLatch ackLatchA = new CountDownLatch(3);
    doAnswer(
            inv -> {
              ackLatchA.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    // Real two-thread pool so the two workers don't serialize.
    singleThreadFactory = n -> Executors.newFixedThreadPool(n);

    CountingSleeper sleeper = new CountingSleeper();
    DataBridge bridge = newBridge(sleeper);
    bridge.register("src", "chanA", "topic-a", consumerA, ChannelOptions.defaults());
    bridge.register("src", "chanB", "topic-b", consumerB, ChannelOptions.defaults());
    bridge.start();

    assertTrue(
        ackLatchA.await(5, TimeUnit.SECONDS),
        "source A should ack >= 3 times while source B is in cooldown");
    // Wait for B's cooldown to actually appear. Under tight scheduling, A can
    // finish 3 acks before B's worker is even scheduled, so we can't infer
    // from A's progress alone that B has reached the breaker.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      synchronized (sleeper.sleeps) {
        if (sleeper.sleeps.contains(cooldown)) {
          break;
        }
      }
      Thread.sleep(10);
    }
    bridge.close();

    verify(consumerA, atLeast(3)).acknowledge(any(Message.class));
    verify(consumerB, never()).acknowledge(any(Message.class));
    long cooldownSleeps;
    synchronized (sleeper.sleeps) {
      cooldownSleeps = sleeper.sleeps.stream().filter(d -> d.equals(cooldown)).count();
    }
    assertTrue(
        cooldownSleeps >= 1,
        "source B should have hit its circuit-breaker at least once; cooldownSleeps="
            + cooldownSleeps);
  }
}
