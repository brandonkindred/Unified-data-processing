package com.unifieddataprocessing.pubsub.gcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcpPubSubPublisherTest {

  @Mock private Publisher mockPublisherA;
  @Mock private Publisher mockPublisherB;

  private List<TopicName> factoryCalls;
  private GcpPubSubPublisher publisher;

  @BeforeEach
  void setUp() {
    factoryCalls = new ArrayList<>();
    GcpPubSubPublisherConfig config = new GcpPubSubPublisherConfig("my-proj");
    publisher =
        new GcpPubSubPublisher(
            config,
            tn -> {
              factoryCalls.add(tn);
              if ("topic-a".equals(tn.getTopic())) {
                return mockPublisherA;
              }
              return mockPublisherB;
            });
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    publisher.connect();
    assertThrows(IllegalStateException.class, publisher::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    Message m = new Message("id", "topic-a", new byte[0], null);
    assertThrows(IllegalStateException.class, () -> publisher.publish(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishSync(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishBatch(List.of(m)));
    assertThrows(IllegalStateException.class, publisher::flush);
  }

  @Test
  void publish_lazilyCreatesPerTopicPublisher() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ackA = SettableApiFuture.create();
    ackA.set("server-id-a");
    SettableApiFuture<String> ackB = SettableApiFuture.create();
    ackB.set("server-id-b");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ackA);
    when(mockPublisherB.publish(any(PubsubMessage.class))).thenReturn(ackB);

    publisher.publish(new Message("a", "topic-a", new byte[] {1}, null)).get();
    publisher.publish(new Message("b", "topic-b", new byte[] {2}, null)).get();

    assertEquals(2, factoryCalls.size());
    assertEquals("topic-a", factoryCalls.get(0).getTopic());
    assertEquals("topic-b", factoryCalls.get(1).getTopic());
    assertEquals("my-proj", factoryCalls.get(0).getProject());
  }

  @Test
  void publish_reusesCachedPublisher() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ack = SettableApiFuture.create();
    ack.set("id");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ack);

    publisher.publish(new Message("a", "topic-a", new byte[] {1}, null)).get();
    publisher.publish(new Message("a2", "topic-a", new byte[] {2}, null)).get();

    assertEquals(1, factoryCalls.size());
    verify(mockPublisherA, times(2)).publish(any(PubsubMessage.class));
  }

  @Test
  void publish_buildsPubsubMessageWithPayloadAndAttributes() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ack = SettableApiFuture.create();
    ack.set("server-id");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ack);

    Message m = new Message("mid", "topic-a", new byte[] {1, 2, 3}, Map.of("k1", "v1", "k2", "v2"));
    final CompletableFuture<PublishResult> future = publisher.publish(m);

    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(mockPublisherA).publish(captor.capture());
    PubsubMessage sent = captor.getValue();
    assertArrayEquals(new byte[] {1, 2, 3}, sent.getData().toByteArray());
    assertEquals("v1", sent.getAttributesMap().get("k1"));
    assertEquals("v2", sent.getAttributesMap().get("k2"));

    PublishResult result = future.get();
    assertEquals("server-id", result.getMessageId());
    assertEquals("topic-a", result.getTopic());
  }

  @Test
  void publish_completesExceptionallyOnApiFailure() {
    publisher.connect();
    SettableApiFuture<String> failed = SettableApiFuture.create();
    RuntimeException error = new RuntimeException("boom");
    failed.setException(error);
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(failed);

    Message m = new Message("a", "topic-a", new byte[0], null);
    CompletableFuture<PublishResult> cf = publisher.publish(m);
    ExecutionException ee = assertThrows(ExecutionException.class, cf::get);
    assertSame(error, ee.getCause());
  }

  @Test
  void publishSync_unwrapsRuntimeException() {
    publisher.connect();
    SettableApiFuture<String> failed = SettableApiFuture.create();
    RuntimeException error = new RuntimeException("boom");
    failed.setException(error);
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(failed);

    Message m = new Message("a", "topic-a", new byte[0], null);
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> publisher.publishSync(m));
    assertSame(error, thrown);
  }

  @Test
  void publishBatch_emptyReturnsEmptyImmediately() throws Exception {
    publisher.connect();
    assertTrue(publisher.publishBatch(List.of()).get().isEmpty());
    verify(mockPublisherA, never()).publish(any(PubsubMessage.class));
  }

  @Test
  void publishBatch_fanOutAcrossTopics() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ackA = SettableApiFuture.create();
    ackA.set("ida");
    SettableApiFuture<String> ackB = SettableApiFuture.create();
    ackB.set("idb");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ackA);
    when(mockPublisherB.publish(any(PubsubMessage.class))).thenReturn(ackB);

    Message ma1 = new Message("a1", "topic-a", new byte[] {1}, null);
    Message mb = new Message("b", "topic-b", new byte[] {2}, null);
    Message ma2 = new Message("a2", "topic-a", new byte[] {3}, null);

    List<PublishResult> results = publisher.publishBatch(List.of(ma1, mb, ma2)).get();
    assertEquals(3, results.size());
    assertEquals(2, factoryCalls.size());
    verify(mockPublisherA, times(2)).publish(any(PubsubMessage.class));
    verify(mockPublisherB, times(1)).publish(any(PubsubMessage.class));
  }

  @Test
  void publishBatch_aggregatesFailures() {
    publisher.connect();
    SettableApiFuture<String> ok1 = SettableApiFuture.create();
    ok1.set("ok1");
    SettableApiFuture<String> ok2 = SettableApiFuture.create();
    ok2.set("ok2");
    SettableApiFuture<String> bad = SettableApiFuture.create();
    RuntimeException error = new RuntimeException("nope");
    bad.setException(error);
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ok1, bad, ok2);

    Message m1 = new Message("a", "topic-a", new byte[] {1}, null);
    Message m2 = new Message("b", "topic-a", new byte[] {2}, null);
    Message m3 = new Message("c", "topic-a", new byte[] {3}, null);

    CompletableFuture<List<PublishResult>> batch = publisher.publishBatch(List.of(m1, m2, m3));
    ExecutionException ee = assertThrows(ExecutionException.class, batch::get);
    PublishBatchException pbe = (PublishBatchException) ee.getCause();
    assertEquals(2, pbe.getSucceeded().size());
    assertSame(error, pbe.getFailures().get(1));
  }

  @Test
  void publishBatch_publisherBuildFailureBecomesAggregatedFailure() {
    // First topic builds OK; second topic's factory throws (e.g. Publisher.Builder.build()
    // raised IOException and the factory wrapped it in UncheckedIOException). The third
    // message reuses the cached publisher for the first topic and must still publish — the
    // sync failure on the middle entry must not abort batch submission.
    GcpPubSubPublisherConfig config = new GcpPubSubPublisherConfig("my-proj");
    UncheckedIOException buildError = new UncheckedIOException(new java.io.IOException("build"));
    GcpPubSubPublisher p =
        new GcpPubSubPublisher(
            config,
            tn -> {
              if ("topic-a".equals(tn.getTopic())) {
                return mockPublisherA;
              }
              throw buildError;
            });
    p.connect();
    SettableApiFuture<String> ack = SettableApiFuture.create();
    ack.set("ok");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ack);

    Message m1 = new Message("a", "topic-a", new byte[] {1}, null);
    Message m2 = new Message("b", "topic-b", new byte[] {2}, null);
    Message m3 = new Message("c", "topic-a", new byte[] {3}, null);

    CompletableFuture<List<PublishResult>> batch = p.publishBatch(List.of(m1, m2, m3));
    ExecutionException ee = assertThrows(ExecutionException.class, batch::get);
    PublishBatchException pbe = (PublishBatchException) ee.getCause();
    assertEquals(2, pbe.getSucceeded().size());
    assertSame(buildError, pbe.getFailures().get(1));
    verify(mockPublisherA, times(2)).publish(any(PubsubMessage.class));
  }

  @Test
  void flush_callsPublishAllOutstandingOnEachCachedPublisher() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ack = SettableApiFuture.create();
    ack.set("id");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ack);
    when(mockPublisherB.publish(any(PubsubMessage.class))).thenReturn(ack);

    publisher.publish(new Message("a", "topic-a", new byte[] {1}, null)).get();
    publisher.publish(new Message("b", "topic-b", new byte[] {2}, null)).get();

    publisher.flush();

    verify(mockPublisherA).publishAllOutstanding();
    verify(mockPublisherB).publishAllOutstanding();
  }

  @Test
  void flush_withNoCachedPublishersIsNoOp() {
    publisher.connect();
    publisher.flush();
    assertNotNull(publisher);
  }

  @Test
  void close_shutsDownAllPublishersAndIsIdempotent() throws Exception {
    publisher.connect();
    SettableApiFuture<String> ack = SettableApiFuture.create();
    ack.set("id");
    when(mockPublisherA.publish(any(PubsubMessage.class))).thenReturn(ack);
    publisher.publish(new Message("a", "topic-a", new byte[] {1}, null)).get();

    publisher.close();
    publisher.close();

    verify(mockPublisherA, times(1)).shutdown();
  }
}
