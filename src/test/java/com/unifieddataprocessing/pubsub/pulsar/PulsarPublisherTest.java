package com.unifieddataprocessing.pubsub.pulsar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PulsarPublisherTest {

  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String TOPIC_A = "topic-a";
  private static final String TOPIC_B = "topic-b";

  @Mock private PulsarClient mockClient;
  @Mock private Producer<byte[]> mockProducerA;
  @Mock private Producer<byte[]> mockProducerB;
  @Mock private TypedMessageBuilder<byte[]> mockBuilderA;
  @Mock private TypedMessageBuilder<byte[]> mockBuilderB;
  @Mock private MessageId mockMessageIdA;
  @Mock private MessageId mockMessageIdB;

  private Map<String, Producer<byte[]>> producerByTopic;
  private List<String> topicsRequested;
  private PulsarPublisherConfig config;
  private PulsarPublisher publisher;

  @BeforeEach
  void setUp() {
    producerByTopic = new LinkedHashMap<>();
    producerByTopic.put(TOPIC_A, mockProducerA);
    producerByTopic.put(TOPIC_B, mockProducerB);
    topicsRequested = new ArrayList<>();
    config = new PulsarPublisherConfig(SERVICE_URL);
    publisher = new PulsarPublisher(config, factory());
  }

  private PulsarPublisher.Factory factory() {
    return new PulsarPublisher.Factory() {
      @Override
      public PulsarClient newClient(PulsarPublisherConfig c) {
        return mockClient;
      }

      @Override
      public Producer<byte[]> newProducer(
          PulsarClient client, PulsarPublisherConfig c, String topic) {
        topicsRequested.add(topic);
        Producer<byte[]> producer = producerByTopic.get(topic);
        if (producer == null) {
          throw new IllegalArgumentException("no mock producer for topic: " + topic);
        }
        return producer;
      }
    };
  }

  private void stubBuilder(Producer<byte[]> producer, TypedMessageBuilder<byte[]> builder) {
    // lenient: not every test exercises both value() and properties(), but the helper
    // primes both because most do. Strict-mode would fail tests that fail early.
    lenient().when(producer.newMessage(Schema.BYTES)).thenReturn(builder);
    lenient().when(builder.value(any(byte[].class))).thenReturn(builder);
    lenient().when(builder.properties(anyMap())).thenReturn(builder);
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    publisher.connect();
    assertThrows(IllegalStateException.class, publisher::connect);
  }

  @Test
  void connect_wrapsPulsarClientException() {
    PulsarPublisher p =
        new PulsarPublisher(
            config,
            new PulsarPublisher.Factory() {
              @Override
              public PulsarClient newClient(PulsarPublisherConfig c) throws PulsarClientException {
                throw new PulsarClientException("boom");
              }

              @Override
              public Producer<byte[]> newProducer(
                  PulsarClient client, PulsarPublisherConfig c, String topic) {
                throw new UnsupportedOperationException();
              }
            });
    assertThrows(UncheckedIOException.class, p::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    Message m = new Message("id", TOPIC_A, new byte[0], null);
    assertThrows(IllegalStateException.class, () -> publisher.publish(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishSync(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishBatch(List.of(m)));
    assertThrows(IllegalStateException.class, publisher::flush);
  }

  @Test
  void publish_lazilyCreatesPerTopicProducer() throws Exception {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    stubBuilder(mockProducerB, mockBuilderB);
    when(mockBuilderA.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdA));
    when(mockBuilderB.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdB));
    when(mockMessageIdA.toString()).thenReturn("ma");
    when(mockMessageIdB.toString()).thenReturn("mb");

    publisher.publish(new Message("a", TOPIC_A, new byte[] {1}, null)).get();
    publisher.publish(new Message("b", TOPIC_B, new byte[] {2}, null)).get();
    publisher.publish(new Message("a2", TOPIC_A, new byte[] {3}, null)).get();

    assertEquals(List.of(TOPIC_A, TOPIC_B), topicsRequested);
    verify(mockProducerA, times(2)).newMessage(Schema.BYTES);
    verify(mockProducerB, times(1)).newMessage(Schema.BYTES);
  }

  @Test
  void publish_buildsTypedMessageWithPayloadAndProperties() throws Exception {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    when(mockBuilderA.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdA));
    when(mockMessageIdA.toString()).thenReturn("msg-1");

    Message m = new Message("a", TOPIC_A, "payload".getBytes(), Map.of("k", "v"));
    final CompletableFuture<PublishResult> future = publisher.publish(m);

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(mockBuilderA).value(valueCaptor.capture());
    assertArrayEquals("payload".getBytes(), valueCaptor.getValue());
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockBuilderA).properties(propsCaptor.capture());
    assertEquals("v", propsCaptor.getValue().get("k"));

    PublishResult r = future.get();

    assertEquals(TOPIC_A + "-msg-1", r.getMessageId());
    assertEquals(TOPIC_A, r.getTopic());
    // sequenceNumber intentionally not surfaced for Pulsar — see PublishResult.forPulsar javadoc
    // and PulsarPublisher.doPublish (the producer-side counter is racy under in-flight sends).
    assertNull(r.getSequenceNumber());
  }

  @Test
  void publish_messageIdIsCapturedFromAck_notFromLaterSends() throws Exception {
    // Regression for the race that motivated dropping getLastSequenceId(): when two sends are in
    // flight on the same producer, the first ack's PublishResult must reflect THAT send's
    // MessageId, not whatever later state the producer has accumulated. Verifies by completing
    // futures out of order and asserting each result carries its own MessageId.
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    final CompletableFuture<MessageId> ack1 = new CompletableFuture<>();
    final CompletableFuture<MessageId> ack2 = new CompletableFuture<>();
    when(mockBuilderA.sendAsync()).thenReturn(ack1, ack2);
    when(mockMessageIdA.toString()).thenReturn("first");
    when(mockMessageIdB.toString()).thenReturn("second");

    final CompletableFuture<PublishResult> r1 =
        publisher.publish(new Message("a", TOPIC_A, new byte[] {1}, null));
    final CompletableFuture<PublishResult> r2 =
        publisher.publish(new Message("b", TOPIC_A, new byte[] {2}, null));

    // Complete the SECOND send's ack first, then the first — out-of-order completion is the
    // exact scenario where reading getLastSequenceId() on r1's callback would see r2's value.
    ack2.complete(mockMessageIdB);
    ack1.complete(mockMessageIdA);

    assertEquals(TOPIC_A + "-first", r1.get().getMessageId());
    assertEquals(TOPIC_A + "-second", r2.get().getMessageId());
  }

  @Test
  void publish_completesExceptionallyOnFailedSendAsync() {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    PulsarClientException error = new PulsarClientException("nope");
    CompletableFuture<MessageId> failed = new CompletableFuture<>();
    failed.completeExceptionally(error);
    when(mockBuilderA.sendAsync()).thenReturn(failed);

    Message m = new Message("a", TOPIC_A, new byte[0], null);
    CompletableFuture<PublishResult> cf = publisher.publish(m);
    ExecutionException ee = assertThrows(ExecutionException.class, cf::get);
    assertSame(error, ee.getCause());
  }

  @Test
  void publishSync_unwrapsRuntimeException() {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    RuntimeException error = new RuntimeException("boom");
    CompletableFuture<MessageId> failed = new CompletableFuture<>();
    failed.completeExceptionally(error);
    when(mockBuilderA.sendAsync()).thenReturn(failed);

    Message m = new Message("a", TOPIC_A, new byte[0], null);
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> publisher.publishSync(m));
    assertSame(error, thrown);
  }

  @Test
  void publishBatch_emptyReturnsEmptyImmediately() throws Exception {
    publisher.connect();
    assertTrue(publisher.publishBatch(List.of()).get().isEmpty());
    verify(mockProducerA, never()).newMessage(Schema.BYTES);
  }

  @Test
  void publishBatch_aggregatesFailures() {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    PulsarClientException error = new PulsarClientException("nope");
    CompletableFuture<MessageId> ok = CompletableFuture.completedFuture(mockMessageIdA);
    CompletableFuture<MessageId> bad = new CompletableFuture<>();
    bad.completeExceptionally(error);
    when(mockBuilderA.sendAsync()).thenReturn(ok, bad, ok);
    when(mockMessageIdA.toString()).thenReturn("m");

    Message m1 = new Message("a", TOPIC_A, new byte[] {1}, null);
    Message m2 = new Message("b", TOPIC_A, new byte[] {2}, null);
    Message m3 = new Message("c", TOPIC_A, new byte[] {3}, null);

    CompletableFuture<List<PublishResult>> batch = publisher.publishBatch(List.of(m1, m2, m3));
    ExecutionException ee = assertThrows(ExecutionException.class, batch::get);
    PublishBatchException pbe = (PublishBatchException) ee.getCause();
    assertEquals(2, pbe.getSucceeded().size());
    assertSame(error, pbe.getFailures().get(1));
  }

  @Test
  void flush_callsFlushAsyncOnAllCachedProducers() throws Exception {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    stubBuilder(mockProducerB, mockBuilderB);
    when(mockBuilderA.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdA));
    when(mockBuilderB.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdB));
    when(mockMessageIdA.toString()).thenReturn("ma");
    when(mockMessageIdB.toString()).thenReturn("mb");
    when(mockProducerA.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));
    when(mockProducerB.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));

    publisher.publish(new Message("a", TOPIC_A, new byte[] {1}, null)).get();
    publisher.publish(new Message("b", TOPIC_B, new byte[] {2}, null)).get();
    publisher.flush();

    verify(mockProducerA).flushAsync();
    verify(mockProducerB).flushAsync();
  }

  @Test
  void flush_withNoCachedProducersIsNoOp() {
    publisher.connect();
    publisher.flush();
    verify(mockProducerA, never()).flushAsync();
  }

  @Test
  void close_closesAllProducersAndClient_bestEffort() throws Exception {
    publisher.connect();
    stubBuilder(mockProducerA, mockBuilderA);
    stubBuilder(mockProducerB, mockBuilderB);
    when(mockBuilderA.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdA));
    when(mockBuilderB.sendAsync()).thenReturn(CompletableFuture.completedFuture(mockMessageIdB));
    when(mockMessageIdA.toString()).thenReturn("ma");
    when(mockMessageIdB.toString()).thenReturn("mb");
    publisher.publish(new Message("a", TOPIC_A, new byte[] {1}, null)).get();
    publisher.publish(new Message("b", TOPIC_B, new byte[] {2}, null)).get();

    // First producer's close throws — second producer and client must still close.
    doThrow(new PulsarClientException("close-a")).when(mockProducerA).close();

    publisher.close();
    publisher.close();

    verify(mockProducerA, times(1)).close();
    verify(mockProducerB, times(1)).close();
    verify(mockClient, times(1)).close();
  }
}
