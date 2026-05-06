package com.unifieddataprocessing.pubsub.pulsar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PulsarConsumerTest {

  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String SUBSCRIPTION = "test-subscription";
  private static final String TOPIC_A = "topic-a";
  private static final String TOPIC_B = "topic-b";

  @Mock private PulsarClient mockClient;
  @Mock private Consumer<byte[]> mockInnerA;
  @Mock private Consumer<byte[]> mockInnerB;
  @Mock private MessageId mockMessageIdA;
  @Mock private MessageId mockMessageIdB;
  @Mock private org.apache.pulsar.client.api.Message<byte[]> mockMsgA;
  @Mock private org.apache.pulsar.client.api.Message<byte[]> mockMsgB;

  private Map<String, Consumer<byte[]>> innerByTopic;
  private AtomicReference<List<Collection<String>>> topicsRequested;
  private PulsarConsumerConfig config;
  private PulsarConsumer consumer;

  @BeforeEach
  void setUp() {
    innerByTopic = new LinkedHashMap<>();
    innerByTopic.put(TOPIC_A, mockInnerA);
    innerByTopic.put(TOPIC_B, mockInnerB);
    topicsRequested = new AtomicReference<>(new ArrayList<>());
    config = new PulsarConsumerConfig(SERVICE_URL, SUBSCRIPTION);
    consumer = new PulsarConsumer(config, factory());
  }

  private PulsarConsumer.Factory factory() {
    return new PulsarConsumer.Factory() {
      @Override
      public PulsarClient newClient(PulsarConsumerConfig c) {
        return mockClient;
      }

      @Override
      public Consumer<byte[]> newConsumer(
          PulsarClient client, PulsarConsumerConfig c, Collection<String> topics) {
        topicsRequested.get().add(new ArrayList<>(topics));
        String first = topics.iterator().next();
        Consumer<byte[]> inner = innerByTopic.get(first);
        if (inner == null) {
          throw new IllegalArgumentException("no mock inner consumer for topic: " + first);
        }
        return inner;
      }
    };
  }

  @Test
  void connect_capturesClientFromFactory() {
    consumer.connect();
    // Idempotency guard verifies connect actually stored the client.
    assertThrows(IllegalStateException.class, consumer::connect);
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    consumer.connect();
    assertThrows(IllegalStateException.class, consumer::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    assertThrows(IllegalStateException.class, () -> consumer.subscribe(TOPIC_A));
    assertThrows(IllegalStateException.class, () -> consumer.unsubscribe(TOPIC_A));
    assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    assertThrows(
        IllegalStateException.class,
        () -> consumer.acknowledge(new Message("id", TOPIC_A, new byte[0], null)));
  }

  @Test
  void subscribe_buildsInnerConsumerForTopic() {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    assertEquals(1, topicsRequested.get().size());
    assertEquals(List.of(TOPIC_A), new ArrayList<>(topicsRequested.get().get(0)));
  }

  @Test
  void subscribe_isIdempotent() {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_A);

    // Factory.newConsumer must have been invoked exactly once.
    assertEquals(1, topicsRequested.get().size());
  }

  @Test
  void subscribe_multiTopic_createsSeparateInnerConsumers() {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);

    assertEquals(2, topicsRequested.get().size());
    assertEquals(List.of(TOPIC_A), new ArrayList<>(topicsRequested.get().get(0)));
    assertEquals(List.of(TOPIC_B), new ArrayList<>(topicsRequested.get().get(1)));
  }

  @Test
  void unsubscribe_closesInnerConsumerForTopic() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);
    consumer.unsubscribe(TOPIC_A);

    verify(mockInnerA, times(1)).close();
    verify(mockInnerB, never()).close();
  }

  @Test
  void unsubscribe_lastTopicLeavesNoInnerConsumers() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.unsubscribe(TOPIC_A);

    // No inner consumer is left to receive from; poll must short-circuit.
    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
    verify(mockInnerA, never()).receive(anyInt(), any(TimeUnit.class));
  }

  @Test
  void unsubscribe_unknownTopicIsNoOp() throws PulsarClientException {
    consumer.connect();
    consumer.unsubscribe("never-subscribed");

    verify(mockInnerA, never()).close();
    verify(mockInnerB, never()).close();
  }

  @Test
  void poll_emptyWhenNoSubscriptions() throws PulsarClientException {
    consumer.connect();
    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
    verify(mockInnerA, never()).receive(anyInt(), any(TimeUnit.class));
    verify(mockInnerB, never()).receive(anyInt(), any(TimeUnit.class));
  }

  @Test
  void poll_emptyWhenInnerReceiveTimesOut() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS))).thenReturn(null);

    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
  }

  @Test
  void poll_mapsPulsarMessageToFrameworkMessage() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("payload".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of("k", "v"));
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    assertEquals(1, messages.size());
    Message m = messages.get(0);
    assertEquals(mockMessageIdA.toString(), m.getId());
    assertEquals(TOPIC_A, m.getTopic());
    assertArrayEquals("payload".getBytes(), m.getPayload());
    assertEquals("v", m.getAttributes().get("k"));
  }

  @Test
  void poll_nullValueBecomesEmptyPayload() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn(null);
    when(mockMsgA.getProperties()).thenReturn(null);
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    assertArrayEquals(new byte[0], m.getPayload());
    assertTrue(m.getAttributes().isEmpty());
  }

  @Test
  void poll_multiTopicMergesMessagesAcrossInnerConsumers() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockMsgB.getMessageId()).thenReturn(mockMessageIdB);
    when(mockMsgB.getValue()).thenReturn("b".getBytes());
    when(mockMsgB.getProperties()).thenReturn(Map.of());

    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);
    when(mockInnerB.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgB)
        .thenReturn(null);

    List<Message> messages = consumer.poll(Duration.ofMillis(20));

    assertEquals(2, messages.size());
    assertEquals(TOPIC_A, messages.get(0).getTopic());
    assertEquals(TOPIC_B, messages.get(1).getTopic());
  }

  @Test
  void poll_respectsMaxMessagesBudget() throws PulsarClientException {
    PulsarConsumerConfig tinyConfig =
        new PulsarConsumerConfig(
            SERVICE_URL,
            SUBSCRIPTION,
            null,
            null,
            1, // maxMessagesPerPoll
            null,
            null,
            null);
    consumer = new PulsarConsumer(tinyConfig, factory());
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS))).thenReturn(mockMsgA);

    List<Message> messages = consumer.poll(Duration.ofMillis(20));

    assertEquals(1, messages.size());
    // budget hit after first message; topic-b's inner consumer should be untouched.
    verify(mockInnerB, never()).receive(anyInt(), any(TimeUnit.class));
  }

  @Test
  void acknowledge_routesToOwnerInnerConsumer() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);
    when(mockInnerB.receive(anyInt(), eq(TimeUnit.MILLISECONDS))).thenReturn(null);

    Message m = consumer.poll(Duration.ofMillis(20)).get(0);
    consumer.acknowledge(m);

    verify(mockInnerA).acknowledge(mockMessageIdA);
    verify(mockInnerB, never()).acknowledge(any(MessageId.class));
  }

  @Test
  void acknowledge_unknownMessageThrows() {
    consumer.connect();
    Message stranger = new Message("not-from-poll", TOPIC_A, new byte[0], null);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(stranger));
  }

  @Test
  void acknowledge_sameMessageTwiceThrowsSecondTime() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    consumer.acknowledge(m);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(m));
  }

  @Test
  void acknowledge_failureLeavesStateForRetry() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);

    // First ack: inner.acknowledge throws (transient). State must remain so a retry succeeds.
    doThrow(new PulsarClientException("transient"))
        .doNothing()
        .when(mockInnerA)
        .acknowledge(any(MessageId.class));

    assertThrows(UncheckedIOException.class, () -> consumer.acknowledge(m));
    consumer.acknowledge(m);

    verify(mockInnerA, times(2)).acknowledge(mockMessageIdA);
  }

  @Test
  void acknowledge_messageIdToStringRoundTripsThroughSideMap() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);

    when(mockMsgA.getMessageId()).thenReturn(mockMessageIdA);
    when(mockMsgA.getValue()).thenReturn("a".getBytes());
    when(mockMsgA.getProperties()).thenReturn(Map.of());
    when(mockInnerA.receive(anyInt(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockMsgA)
        .thenReturn(null);

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    // Caller-constructed Message with the same id string must resolve to the same MessageId
    // instance in the side-map and route to the same inner consumer.
    Message rebuilt = new Message(m.getId(), m.getTopic(), m.getPayload(), m.getAttributes());
    consumer.acknowledge(rebuilt);

    verify(mockInnerA).acknowledge(mockMessageIdA);
  }

  @Test
  void close_closesAllInnerConsumersAndClient() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);
    consumer.close();

    verify(mockInnerA).close();
    verify(mockInnerB).close();
    verify(mockClient).close();
  }

  @Test
  void close_isIdempotent() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.close();
    consumer.close();

    verify(mockInnerA, times(1)).close();
    verify(mockClient, times(1)).close();
  }

  @Test
  void close_doesNotPropagateInnerCloseException() throws PulsarClientException {
    consumer.connect();
    consumer.subscribe(TOPIC_A);
    consumer.subscribe(TOPIC_B);

    doThrow(new PulsarClientException("a-close-fail")).when(mockInnerA).close();

    consumer.close(); // must not throw

    verify(mockInnerB).close();
    verify(mockClient).close();
  }
}
