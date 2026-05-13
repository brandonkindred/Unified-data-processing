package com.unifieddataprocessing.pubsub.relay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataRelayTest {

  @Mock private PubSubConsumer consumerA;
  @Mock private PubSubConsumer consumerB;
  @Mock private PubSubPublisher publisherA;
  @Mock private PubSubPublisher publisherB;

  private Function<Integer, ExecutorService> singleThreadFactory;
  private Sleeper noopSleeper;
  private DataRelayConfig config;

  @BeforeEach
  void setUp() {
    singleThreadFactory = n -> Executors.newSingleThreadExecutor();
    noopSleeper = d -> {};
    config =
        DataRelayConfig.builder()
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(500))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .build();
  }

  private DataRelay newRelay() {
    return new DataRelay(config, singleThreadFactory, noopSleeper);
  }

  private DataRelay newRelay(Sleeper sleeper) {
    return new DataRelay(config, singleThreadFactory, sleeper);
  }

  /** Records every {@link Sleeper#sleep(Duration)} invocation; thread-safe for worker threads. */
  static final class CountingSleeper implements Sleeper {
    final List<Duration> sleeps = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void sleep(Duration d) {
      sleeps.add(d);
    }
  }

  private static Message msg(String id, String topic, Map<String, String> attrs) {
    return new Message(id, topic, new byte[] {1, 2, 3}, attrs);
  }

  @Test
  void happyPath_connectsSubscribesPublishesAcksInOrder() throws Exception {
    Message source = msg("m-1", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(source))
        .thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "consumerA should have acked the message");
    relay.close();

    InOrder order = inOrder(publisherA, consumerA);
    order.verify(publisherA).connect();
    order.verify(consumerA).connect();
    order.verify(consumerA).subscribe("shopify.orders");
    order.verify(consumerA).poll(any(Duration.class));
    order.verify(publisherA).publish(any(Message.class));
    order.verify(consumerA).acknowledge(any(Message.class));
  }

  @Test
  void happyPath_rewrittenMessageHasDownstreamTopicAndRelayAttrs() throws Exception {
    Map<String, String> callerAttrs = new LinkedHashMap<>();
    callerAttrs.put(RelayAttributes.RELAY_DESTINATION_ID, "wrong");
    callerAttrs.put("bridge.sourceId", "shopify"); // preserved from inbound bridge
    callerAttrs.put("kafkaKey", "k-1");
    Message source = msg("m-1", "shopify.orders", callerAttrs);
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(source))
        .thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    relay.close();

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(publisherA).publish(captor.capture());
    Message published = captor.getValue();
    assertEquals("orders_q", published.getTopic());
    assertEquals(
        "rabbit-prod", published.getAttributes().get(RelayAttributes.RELAY_DESTINATION_ID));
    assertEquals(
        "shopify.orders", published.getAttributes().get(RelayAttributes.RELAY_SOURCE_TOPIC));
    assertEquals(
        "orders_q", published.getAttributes().get(RelayAttributes.RELAY_DOWNSTREAM_TOPIC));
    assertEquals("shopify", published.getAttributes().get("bridge.sourceId"));
    assertEquals("k-1", published.getAttributes().get("kafkaKey"));
    assertEquals("m-1", published.getId());
  }

  @Test
  void twoDestinations_eachPublishedAndAcked() throws Exception {
    Message messageA = msg("m-A", "shopify.orders", Map.of());
    Message messageB = msg("m-B", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class))).thenReturn(List.of(messageA)).thenReturn(List.of());
    when(consumerB.poll(any(Duration.class))).thenReturn(List.of(messageB)).thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(publisherB.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

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

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.orders", "orders-topic", consumerB, publisherB);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "both registrations should have acked");
    relay.close();

    ArgumentCaptor<Message> captorA = ArgumentCaptor.forClass(Message.class);
    ArgumentCaptor<Message> captorB = ArgumentCaptor.forClass(Message.class);
    verify(publisherA).publish(captorA.capture());
    verify(publisherB).publish(captorB.capture());
    Set<String> publishedTopics =
        List.of(captorA.getValue(), captorB.getValue()).stream()
            .map(Message::getTopic)
            .collect(Collectors.toSet());
    assertEquals(Set.of("orders_q", "orders-topic"), publishedTopics);
  }

  @Test
  void pollThrowsThenRecovers() throws Exception {
    Message source = msg("m-1", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenThrow(new RuntimeException("transient poll error"))
        .thenReturn(List.of(source))
        .thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    CountingSleeper sleeper = new CountingSleeper();
    DataRelay relay = newRelay(sleeper);
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "consumerA should ack after poll recovers");
    relay.close();

    verify(publisherA, times(1)).publish(any(Message.class));
    verify(consumerA, times(1)).acknowledge(any(Message.class));
    assertEquals(List.of(config.pollBackoff()), sleeper.sleeps);
  }

  @Test
  void publishTimesOut_pausesAndRetriesSameMessage_noFurtherPoll() throws Exception {
    // Shorten publishTimeout so the timed-out get() returns quickly.
    config =
        DataRelayConfig.builder()
            .pollTimeout(Duration.ofMillis(50))
            .publishTimeout(Duration.ofMillis(100))
            .shutdownTimeout(Duration.ofMillis(500))
            .closeForceTimeout(Duration.ofMillis(200))
            .pollBackoff(Duration.ofMillis(50))
            .build();

    Message m1 = msg("m-1", "shopify.orders", Map.of());
    Message m2 = msg("m-2", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of());

    // First publish never completes (TimeoutException); subsequent calls succeed.
    // The relay should retry the SAME message (m1) without polling again, then publish m2.
    CompletableFuture<PublishResult> neverCompletes = new CompletableFuture<>();
    when(publisherA.publish(any(Message.class)))
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

    CountingSleeper sleeper = new CountingSleeper();
    DataRelay relay = newRelay(sleeper);
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS), "both messages should ack after retry");
    relay.close();

    // 3 publishes: m1 timeout + m1 retry-success + m2 success. 2 acks.
    verify(publisherA, times(3)).publish(any(Message.class));
    verify(consumerA, times(2)).acknowledge(any(Message.class));
    // Exactly one backoff sleep, for the single publish failure.
    assertEquals(List.of(config.pollBackoff()), sleeper.sleeps);
  }

  @Test
  void publishFails_pausesAndRetriesSameMessage_noFurtherPoll() throws Exception {
    Message m1 = msg("m-1", "shopify.orders", Map.of());
    Message m2 = msg("m-2", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m1, m2))
        .thenReturn(List.of());

    CompletableFuture<PublishResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("publish-fail"));
    when(publisherA.publish(any(Message.class)))
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

    CountingSleeper sleeper = new CountingSleeper();
    DataRelay relay = newRelay(sleeper);
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(3, TimeUnit.SECONDS), "both messages should ack after retry");
    relay.close();

    verify(publisherA, times(3)).publish(any(Message.class));
    verify(consumerA, times(2)).acknowledge(any(Message.class));
    assertEquals(List.of(config.pollBackoff()), sleeper.sleeps);
  }

  @Test
  void acknowledgeFails_backsOffAndRetries_workerStaysAlive() throws Exception {
    // First poll returns one message; second poll returns empty. Without ack retry, a thrown
    // RuntimeException from acknowledge() would escape the worker lambda and silently kill the
    // poll loop while isRunning() still returned true.
    Message m1 = msg("m-1", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m1))
        .thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(1);
    AtomicInteger ackAttempts = new AtomicInteger();
    doAnswer(
            inv -> {
              int n = ackAttempts.incrementAndGet();
              if (n == 1) {
                throw new RuntimeException("transient commit failure");
              }
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    CountingSleeper sleeper = new CountingSleeper();
    DataRelay relay = newRelay(sleeper);
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "ack should succeed on retry");
    assertTrue(relay.isRunning(), "worker should still be alive after transient ack failure");
    relay.close();

    // 2 ack attempts: first throws, retry succeeds. 1 backoff sleep.
    verify(consumerA, times(2)).acknowledge(any(Message.class));
    assertEquals(List.of(config.pollBackoff()), sleeper.sleeps);
  }

  @Test
  void failingDestinationDoesNotBlockHealthyDestination() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    when(consumerA.poll(any(Duration.class)))
        .thenAnswer(
            inv -> List.of(msg("a-" + counter.incrementAndGet(), "shopify.orders", Map.of())));
    when(consumerB.poll(any(Duration.class)))
        .thenAnswer(
            inv -> {
              throw new RuntimeException("destination-b consumer broken");
            });
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    CountDownLatch ackLatch = new CountDownLatch(3);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    // Real two-thread pool so the failing registration can't starve the healthy one.
    singleThreadFactory = n -> Executors.newFixedThreadPool(n);

    CountingSleeper sleeper = new CountingSleeper();
    DataRelay relay = newRelay(sleeper);
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.orders", "orders-topic", consumerB, publisherB);
    relay.start();
    assertTrue(ackLatch.await(5, TimeUnit.SECONDS), "destination A should ack >= 3 times");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (sleeper.sleeps.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }

    long t0 = System.nanoTime();
    relay.close();
    long elapsedNs = System.nanoTime() - t0;

    verify(consumerA, atLeast(3)).acknowledge(any(Message.class));
    assertTrue(
        sleeper.sleeps.size() >= 1,
        "destination B's poll failures should have triggered at least one backoff sleep");

    Duration budget =
        config.shutdownTimeout().plus(config.closeForceTimeout()).plus(Duration.ofMillis(500));
    assertTrue(
        elapsedNs <= budget.toNanos(),
        "close() took " + Duration.ofNanos(elapsedNs) + " but budget is " + budget);
  }

  @Test
  void register_afterStart_throwsIllegalStateException() {
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertTrue(relay.isRunning());

    assertThrows(
        IllegalStateException.class,
        () ->
            relay.register(
                "pulsar-prod", "shopify.orders", "orders-topic", consumerB, publisherB));

    relay.close();
  }

  @Test
  void register_duplicateDestinationIdSourceTopicPair_throwsIllegalArgumentException() {
    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            relay.register(
                "rabbit-prod", "shopify.orders", "orders_q_2", consumerB, publisherB));
  }

  @Test
  void register_sameSourceTopic_differentDestination_accepted() {
    // Fan-out: one Kafka topic relayed to two different destinations.
    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.orders", "orders-topic", consumerB, publisherB);
  }

  @Test
  void register_duplicateConsumerInstance_throwsIllegalArgumentException() {
    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            relay.register(
                "pulsar-prod", "shopify.payments", "payments-topic", consumerA, publisherB));
  }

  @Test
  void register_duplicatePublisherInstance_throwsIllegalArgumentException() {
    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            relay.register(
                "pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherA));
  }

  @Test
  void register_invalidDestinationId_throwsIllegalArgumentException() {
    DataRelay relay = newRelay();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            relay.register("rabbit.prod", "shopify.orders", "orders_q", consumerA, publisherA));
  }

  @Test
  void start_twice_throwsIllegalStateException() {
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    assertThrows(IllegalStateException.class, relay::start);
    relay.close();
  }

  @Test
  void start_withNoRegistrations_throwsIllegalStateException() {
    DataRelay relay = newRelay();
    assertThrows(IllegalStateException.class, relay::start);
  }

  @Test
  void start_subscribeFailure_cleansUpConnectedConsumersAndPublishers_stateClosed() {
    doThrow(new RuntimeException("subscribe boom")).when(consumerB).subscribe("shopify.payments");

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);

    assertThrows(RuntimeException.class, relay::start);
    assertFalse(relay.isRunning());

    relay.close();

    // Both publishers connect first; both consumers connect; consumerB's subscribe throws.
    verify(publisherA).connect();
    verify(publisherB).connect();
    verify(consumerA).connect();
    verify(consumerB).connect();
    verify(consumerA, times(1)).close();
    verify(consumerB, times(1)).close();
    verify(publisherA, times(1)).close();
    verify(publisherB, times(1)).close();
  }

  @Test
  void start_publisherConnectFailure_doesNotConnectConsumers_stateClosed() {
    doThrow(new RuntimeException("publisher connect boom")).when(publisherB).connect();

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);

    assertThrows(RuntimeException.class, relay::start);
    assertFalse(relay.isRunning());

    relay.close();

    // publisherA connected successfully before publisherB failed; no consumers ever connected.
    verify(publisherA).connect();
    verify(publisherB).connect();
    verify(consumerA, never()).connect();
    verify(consumerB, never()).connect();
    verify(publisherA, times(1)).close();
    verify(publisherB, never()).close();
  }

  @Test
  void close_flushesEveryPublisherBeforeClosingThem() {
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);
    relay.start();
    relay.close();

    InOrder orderA = inOrder(publisherA);
    orderA.verify(publisherA).flush();
    orderA.verify(publisherA).close();

    InOrder orderB = inOrder(publisherB);
    orderB.verify(publisherB).flush();
    orderB.verify(publisherB).close();
  }

  @Test
  void close_tolerates_publisherThatThrowsOnFlush() throws Exception {
    doThrow(new RuntimeException("flush boom")).when(publisherA).flush();
    Message m = msg("m-1", "shopify.orders", Map.of());
    when(consumerA.poll(any(Duration.class)))
        .thenReturn(List.of(m))
        .thenReturn(List.of());
    when(publisherA.publish(any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    CountDownLatch ackLatch = new CountDownLatch(1);
    doAnswer(
            inv -> {
              ackLatch.countDown();
              return null;
            })
        .when(consumerA)
        .acknowledge(any(Message.class));

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);
    relay.start();
    assertTrue(ackLatch.await(2, TimeUnit.SECONDS));
    relay.close();

    verify(consumerA).close();
    verify(consumerB).close();
    verify(publisherA).close();
    verify(publisherB).close();
  }

  @Test
  void close_boundsFlushPhaseAgainstCloseForceTimeout() throws Exception {
    // publisherA.flush() blocks forever (latch never decremented); close() must still return
    // within the relay's declared budget instead of stalling on the publisher.
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    CountDownLatch flushReleaser = new CountDownLatch(1);
    doAnswer(
            inv -> {
              flushReleaser.await(60, TimeUnit.SECONDS);
              return null;
            })
        .when(publisherA)
        .flush();

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);
    relay.start();

    long t0 = System.nanoTime();
    relay.close();
    long elapsedNs = System.nanoTime() - t0;
    flushReleaser.countDown(); // release the hung flush so the daemon thread can finish

    // Worst case: shutdownTimeout (executor graceful) + closeForceTimeout (flush phase) + grace.
    Duration budget =
        config.shutdownTimeout().plus(config.closeForceTimeout()).plus(Duration.ofMillis(500));
    assertTrue(
        elapsedNs <= budget.toNanos(),
        "close() took " + Duration.ofNanos(elapsedNs) + " but budget is " + budget);

    // close() on publishers and consumers is still attempted (best-effort), even after the
    // hanging flush is abandoned.
    verify(publisherA).close();
    verify(publisherB).close();
    verify(consumerA).close();
    verify(consumerB).close();
  }

  @Test
  void close_stuckFlushOnA_doesNotBlockFlushOnB() throws Exception {
    // publisherA.flush() hangs (and would ignore Thread.interrupt() — the latch only releases
    // when the test releases it). publisherB.flush() must still get a fair shot at the remaining
    // budget despite A holding onto its own flush thread.
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());
    lenient().when(consumerB.poll(any(Duration.class))).thenReturn(List.of());

    CountDownLatch flushReleaser = new CountDownLatch(1);
    doAnswer(
            inv -> {
              // Loop on a non-interruptible await so a Thread.interrupt() is genuinely ignored,
              // mirroring publishers (GCP/Kinesis) whose flush() doesn't honor interruption.
              while (!flushReleaser.await(50, TimeUnit.MILLISECONDS)) {
                // swallow any interrupt status and keep waiting
                Thread.interrupted();
              }
              return null;
            })
        .when(publisherA)
        .flush();

    CountDownLatch flushedB = new CountDownLatch(1);
    doAnswer(
            inv -> {
              flushedB.countDown();
              return null;
            })
        .when(publisherB)
        .flush();

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.register("pulsar-prod", "shopify.payments", "payments-topic", consumerB, publisherB);
    relay.start();

    long t0 = System.nanoTime();
    relay.close();
    long elapsedNs = System.nanoTime() - t0;
    flushReleaser.countDown(); // release the hung flush so the daemon thread can finish

    // Worst case budget: shutdownTimeout + closeForceTimeout + grace.
    Duration budget =
        config.shutdownTimeout().plus(config.closeForceTimeout()).plus(Duration.ofMillis(500));
    assertTrue(
        elapsedNs <= budget.toNanos(),
        "close() took " + Duration.ofNanos(elapsedNs) + " but budget is " + budget);

    // Despite publisherA's flush() ignoring interruption, publisherB.flush() was reached and ran.
    assertTrue(
        flushedB.await(1, TimeUnit.SECONDS),
        "publisherB.flush() should have run despite publisherA.flush() hanging");
    verify(publisherA).close();
    verify(publisherB).close();
  }

  @Test
  void close_calledTwice_isNoOp() {
    lenient().when(consumerA.poll(any(Duration.class))).thenReturn(List.of());

    DataRelay relay = newRelay();
    relay.register("rabbit-prod", "shopify.orders", "orders_q", consumerA, publisherA);
    relay.start();
    relay.close();
    relay.close();

    verify(publisherA, times(1)).close();
    verify(consumerA, times(1)).close();
  }
}
