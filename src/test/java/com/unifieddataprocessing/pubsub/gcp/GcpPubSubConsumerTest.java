package com.unifieddataprocessing.pubsub.gcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.unifieddataprocessing.pubsub.Message;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcpPubSubConsumerTest {

  private static final String PROJECT = "test-project";
  private static final String SUBSCRIPTION = "test-subscription";
  private static final String SUBSCRIPTION_PATH =
      "projects/" + PROJECT + "/subscriptions/" + SUBSCRIPTION;

  @Mock private SubscriberStub mockStub;
  @Mock private UnaryCallable<PullRequest, PullResponse> mockPullCallable;
  @Mock private UnaryCallable<AcknowledgeRequest, Empty> mockAckCallable;

  private AtomicReference<SubscriberStubSettings> capturedSettings;
  private GcpPubSubConsumerConfig config;
  private GcpPubSubConsumer consumer;

  @BeforeEach
  void setUp() {
    capturedSettings = new AtomicReference<>();
    config = new GcpPubSubConsumerConfig(PROJECT, SUBSCRIPTION);
    consumer =
        new GcpPubSubConsumer(
            config,
            settings -> {
              capturedSettings.set(settings);
              return mockStub;
            });
  }

  private void stubPull(PullResponse response) {
    when(mockStub.pullCallable()).thenReturn(mockPullCallable);
    when(mockPullCallable.call(any(PullRequest.class), any(ApiCallContext.class)))
        .thenReturn(response);
  }

  private void stubAck() {
    when(mockStub.acknowledgeCallable()).thenReturn(mockAckCallable);
    when(mockAckCallable.call(any(AcknowledgeRequest.class)))
        .thenReturn(Empty.getDefaultInstance());
  }

  private static ReceivedMessage receivedMessage(String messageId, String ackId, String payload) {
    return ReceivedMessage.newBuilder()
        .setAckId(ackId)
        .setMessage(
            PubsubMessage.newBuilder()
                .setMessageId(messageId)
                .setData(ByteString.copyFromUtf8(payload))
                .putAttributes("k", "v")
                .build())
        .build();
  }

  @Test
  void connect_capturesSettingsAndStoresStub() {
    consumer.connect();
    assertNotNull(capturedSettings.get());
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    consumer.connect();
    assertThrows(IllegalStateException.class, consumer::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    assertThrows(IllegalStateException.class, () -> consumer.subscribe(SUBSCRIPTION));
    assertThrows(IllegalStateException.class, () -> consumer.unsubscribe(SUBSCRIPTION));
    assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    assertThrows(
        IllegalStateException.class,
        () -> consumer.acknowledge(new Message("id", "t", new byte[0], null)));
  }

  @Test
  void subscribe_acceptsConfiguredSubscriptionOnly() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION); // happy path

    assertThrows(
        IllegalArgumentException.class, () -> consumer.subscribe("some-other-subscription"));
  }

  @Test
  void subscribe_isIdempotent() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);
    consumer.subscribe(SUBSCRIPTION);
    // Subscribe never issues an RPC for Pub/Sub — verify no pull/ack was triggered.
    verify(mockStub, never()).pullCallable();
    verify(mockStub, never()).acknowledgeCallable();
  }

  @Test
  void unsubscribe_removesAndIsNoOpForUnknown() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);
    consumer.unsubscribe(SUBSCRIPTION);
    consumer.unsubscribe("never-subscribed"); // no-op, must not throw
  }

  @Test
  void poll_withoutSubscriptionReturnsEmptyList() {
    consumer.connect();
    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
    // No RPC should have been issued.
    verify(mockStub, never()).pullCallable();
  }

  @Test
  void poll_emptyResponseReturnsEmptyList() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);
    stubPull(PullResponse.getDefaultInstance());

    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
  }

  @Test
  void poll_mapsReceivedMessagesAndStashesAckIds() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("m-1", "ack-1", "payload"))
            .build();
    stubPull(response);

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    assertEquals(1, messages.size());
    Message m = messages.get(0);
    assertEquals("m-1", m.getId());
    assertEquals(SUBSCRIPTION, m.getTopic());
    assertArrayEquals("payload".getBytes(), m.getPayload());
    assertEquals("v", m.getAttributes().get("k"));

    ArgumentCaptor<PullRequest> reqCaptor = ArgumentCaptor.forClass(PullRequest.class);
    verify(mockPullCallable).call(reqCaptor.capture(), any(ApiCallContext.class));
    PullRequest sent = reqCaptor.getValue();
    assertEquals(SUBSCRIPTION_PATH, sent.getSubscription());
    assertEquals(GcpPubSubConsumerConfig.DEFAULT_MAX_MESSAGES_PER_POLL, sent.getMaxMessages());
  }

  @Test
  void acknowledge_callsAckRpcWithStashedAckId() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    stubPull(
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("m-1", "ack-1", "x"))
            .build());
    Message m = consumer.poll(Duration.ofMillis(10)).get(0);

    stubAck();
    consumer.acknowledge(m);

    ArgumentCaptor<AcknowledgeRequest> reqCaptor =
        ArgumentCaptor.forClass(AcknowledgeRequest.class);
    verify(mockAckCallable).call(reqCaptor.capture());
    AcknowledgeRequest sent = reqCaptor.getValue();
    assertEquals(SUBSCRIPTION_PATH, sent.getSubscription());
    assertEquals(List.of("ack-1"), sent.getAckIdsList());
  }

  @Test
  void acknowledge_unknownMessageThrows() {
    consumer.connect();
    Message stranger = new Message("not-from-poll", "t", new byte[0], null);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(stranger));
  }

  @Test
  void acknowledge_sameMessageTwiceThrowsSecondTime() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    stubPull(
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("m-1", "ack-1", "x"))
            .build());
    Message m = consumer.poll(Duration.ofMillis(10)).get(0);

    stubAck();
    consumer.acknowledge(m);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(m));
  }

  @Test
  void acknowledge_failureLeavesStateForRetry() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    stubPull(
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("m-1", "ack-1", "x"))
            .build());
    Message m = consumer.poll(Duration.ofMillis(10)).get(0);

    when(mockStub.acknowledgeCallable()).thenReturn(mockAckCallable);
    when(mockAckCallable.call(any(AcknowledgeRequest.class)))
        .thenThrow(new RuntimeException("transient"))
        .thenReturn(Empty.getDefaultInstance());

    assertThrows(RuntimeException.class, () -> consumer.acknowledge(m));
    // Side-map entry must still be present so a retry on the same Message succeeds.
    consumer.acknowledge(m);

    verify(mockAckCallable, times(2)).call(any(AcknowledgeRequest.class));
  }

  @Test
  void close_closesStubAndIsIdempotent() {
    consumer.connect();
    consumer.close();
    consumer.close();

    verify(mockStub, times(1)).close();
  }

  @Test
  void poll_redeliveryUpdatesAckId() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    when(mockStub.pullCallable()).thenReturn(mockPullCallable);
    when(mockPullCallable.call(any(PullRequest.class), any(ApiCallContext.class)))
        .thenReturn(
            PullResponse.newBuilder()
                .addReceivedMessages(receivedMessage("m-1", "ack-A", "x"))
                .build())
        .thenReturn(
            PullResponse.newBuilder()
                .addReceivedMessages(receivedMessage("m-1", "ack-B", "x"))
                .build());

    consumer.poll(Duration.ofMillis(10));
    Message redelivered = consumer.poll(Duration.ofMillis(10)).get(0);

    stubAck();
    consumer.acknowledge(redelivered);

    ArgumentCaptor<AcknowledgeRequest> reqCaptor =
        ArgumentCaptor.forClass(AcknowledgeRequest.class);
    verify(mockAckCallable).call(reqCaptor.capture());
    // Latest ackId — not the original ack-A — is what we send.
    assertEquals(List.of("ack-B"), reqCaptor.getValue().getAckIdsList());
  }

  @Test
  void poll_propagatesRpcFailure() {
    consumer.connect();
    consumer.subscribe(SUBSCRIPTION);

    when(mockStub.pullCallable()).thenReturn(mockPullCallable);
    when(mockPullCallable.call(any(PullRequest.class), any(ApiCallContext.class)))
        .thenThrow(new RuntimeException("rpc failed"));

    assertThrows(RuntimeException.class, () -> consumer.poll(Duration.ofMillis(10)));
  }
}
