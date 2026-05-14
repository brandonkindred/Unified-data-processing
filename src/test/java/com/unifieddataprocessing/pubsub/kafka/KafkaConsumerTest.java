package com.unifieddataprocessing.pubsub.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerTest {

  @Mock private Consumer<byte[], byte[]> mockKafkaClient;

  private AtomicReference<Properties> capturedProps;
  private KafkaConsumerConfig config;
  private KafkaConsumer consumer;

  @BeforeEach
  void setUp() {
    capturedProps = new AtomicReference<>();
    config = new KafkaConsumerConfig("broker:9092", "test-group", Map.of("max.poll.records", 100));
    consumer =
        new KafkaConsumer(
            config,
            props -> {
              capturedProps.set(props);
              return mockKafkaClient;
            });
  }

  @Test
  void connect_appliesFrameworkOverridesAndUserConfig() {
    consumer.connect();

    Properties props = capturedProps.get();
    assertNotNull(props);
    assertEquals("broker:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("test-group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals(100, props.get("max.poll.records"));
    assertEquals(
        ByteArrayDeserializer.class.getName(),
        props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(
        ByteArrayDeserializer.class.getName(),
        props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertEquals("false", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    consumer.connect();
    assertThrows(IllegalStateException.class, consumer::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    assertThrows(IllegalStateException.class, () -> consumer.subscribe("t"));
    assertThrows(IllegalStateException.class, () -> consumer.unsubscribe("t"));
    assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    assertThrows(
        IllegalStateException.class,
        () -> consumer.acknowledge(new Message("id", "t", new byte[0], null)));
  }

  @Test
  @SuppressWarnings("unchecked")
  void subscribe_passesUnionOfTopicsToClient() {
    consumer.connect();
    consumer.subscribe("topic-a");
    consumer.subscribe("topic-b");

    ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockKafkaClient, org.mockito.Mockito.times(2)).subscribe(captor.capture());
    assertEquals(Set.of("topic-a"), captor.getAllValues().get(0));
    assertEquals(Set.of("topic-a", "topic-b"), captor.getAllValues().get(1));
  }

  @Test
  void subscribe_isIdempotent() {
    consumer.connect();
    consumer.subscribe("topic-a");
    consumer.subscribe("topic-a");

    // only one subscribe call past the first
    verify(mockKafkaClient, org.mockito.Mockito.times(1)).subscribe(any(Set.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void unsubscribe_removesTopicAndCallsClient() {
    consumer.connect();
    consumer.subscribe("topic-a");
    consumer.subscribe("topic-b");
    consumer.unsubscribe("topic-a");

    ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockKafkaClient, org.mockito.Mockito.times(3)).subscribe(captor.capture());
    assertEquals(Set.of("topic-b"), captor.getAllValues().get(2));
  }

  @Test
  void unsubscribe_lastTopicCallsClientUnsubscribe() {
    consumer.connect();
    consumer.subscribe("topic-a");
    consumer.unsubscribe("topic-a");

    verify(mockKafkaClient).unsubscribe();
  }

  @Test
  void unsubscribe_unknownTopicIsNoOp() {
    consumer.connect();
    consumer.unsubscribe("never-subscribed");

    verify(mockKafkaClient, never()).unsubscribe();
    verify(mockKafkaClient, never()).subscribe(any(Set.class));
  }

  @Test
  void poll_emptyRecordsReturnsEmptyList() {
    consumer.connect();
    when(mockKafkaClient.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
  }

  @Test
  void poll_mapsRecordsToMessagesAndTracksOffsets() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 3);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("topic-a", 3, 42L, "k".getBytes(), "payload".getBytes());
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp, List.of(record)));
    when(mockKafkaClient.poll(any(Duration.class))).thenReturn(records);

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    assertEquals(1, messages.size());
    Message m = messages.get(0);
    assertEquals("topic-a-3-42", m.getId());
    assertEquals("topic-a", m.getTopic());
    assertArrayEquals("payload".getBytes(), m.getPayload());
  }

  @Test
  void poll_nullValueBecomesEmptyPayload() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic-a", 0, 0L, null, null);
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(record))));

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    assertArrayEquals(new byte[0], m.getPayload());
  }

  @Test
  void acknowledge_commitsOffsetPlusOne() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 3);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("topic-a", 3, 42L, null, "payload".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(record))));

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    consumer.acknowledge(m);

    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(43L))));
  }

  @Test
  void acknowledge_unknownMessageThrows() {
    consumer.connect();
    Message stranger = new Message("not-from-poll", "topic-a", new byte[0], null);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(stranger));
  }

  @Test
  void acknowledge_sameMessageTwiceThrowsSecondTime() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("topic-a", 0, 7L, null, "x".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(record))));

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);
    consumer.acknowledge(m);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(m));
  }

  @Test
  void close_closesUnderlyingClientAndIsIdempotent() {
    consumer.connect();
    consumer.close();
    consumer.close();

    verify(mockKafkaClient, org.mockito.Mockito.times(1)).close();
  }

  @Test
  void acknowledge_outOfOrderDoesNotRegressCommit() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r41 =
        new ConsumerRecord<>("topic-a", 0, 41L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r42 =
        new ConsumerRecord<>("topic-a", 0, 42L, null, "b".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r41, r42))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    Message m41 = messages.get(0);
    Message m42 = messages.get(1);

    // Ack the later offset first; nothing should be committed yet because
    // 41 is still in flight.
    consumer.acknowledge(m42);
    verify(mockKafkaClient, never()).commitSync(any(Map.class));

    // Ack the earlier offset; the watermark advances past both records, so
    // a single commit lands at offset 43 — never at 42 (which would regress).
    consumer.acknowledge(m41);
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(43L))));
    verify(mockKafkaClient, org.mockito.Mockito.times(1)).commitSync(any(Map.class));
  }

  @Test
  void acknowledge_gapDoesNotSkipUnacked() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r43 =
        new ConsumerRecord<>("topic-a", 0, 43L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r44 =
        new ConsumerRecord<>("topic-a", 0, 44L, null, "b".getBytes());
    ConsumerRecord<byte[], byte[]> r45 =
        new ConsumerRecord<>("topic-a", 0, 45L, null, "c".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r43, r44, r45))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    Message m43 = messages.get(0);
    Message m45 = messages.get(2);

    consumer.acknowledge(m45);
    verify(mockKafkaClient, never()).commitSync(any(Map.class));

    consumer.acknowledge(m43);
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(44L))));

    Message m44 = messages.get(1);
    consumer.acknowledge(m44);
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(46L))));
    verify(mockKafkaClient, org.mockito.Mockito.times(2)).commitSync(any(Map.class));
  }

  @Test
  void acknowledge_commitFailureLeavesStateForRetry() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r50 =
        new ConsumerRecord<>("topic-a", 0, 50L, null, "x".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r50))));

    Message m = consumer.poll(Duration.ofMillis(10)).get(0);

    // First ack: commitSync throws, so the watermark must NOT advance and
    // the acked-set must still hold offset 50.
    doThrow(new CommitFailedException()).when(mockKafkaClient).commitSync(any(Map.class));
    assertThrows(CommitFailedException.class, () -> consumer.acknowledge(m));

    // Second ack on the same Message: this time commitSync succeeds. We
    // expect a fresh commit attempt to offset 51 — proving the retry path
    // re-attempts the commit instead of silently dropping the ack.
    doNothing().when(mockKafkaClient).commitSync(any(Map.class));
    consumer.acknowledge(m);

    verify(mockKafkaClient, org.mockito.Mockito.times(2))
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(51L))));
  }

  @Test
  void acknowledge_offsetGapsDoNotBlockWatermark() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    // Compacted/transactional topic: offsets 101, 103, 104 don't reach the
    // consumer (compaction holes or control records). Delivered: 100, 102, 105.
    ConsumerRecord<byte[], byte[]> r100 =
        new ConsumerRecord<>("topic-a", 0, 100L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r102 =
        new ConsumerRecord<>("topic-a", 0, 102L, null, "b".getBytes());
    ConsumerRecord<byte[], byte[]> r105 =
        new ConsumerRecord<>("topic-a", 0, 105L, null, "c".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r100, r102, r105))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(101L))));
    consumer.acknowledge(messages.get(1));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(103L))));
    consumer.acknowledge(messages.get(2));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(106L))));
  }

  @Test
  void acknowledge_offsetGapsWithOutOfOrderAcks() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r100 =
        new ConsumerRecord<>("topic-a", 0, 100L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r102 =
        new ConsumerRecord<>("topic-a", 0, 102L, null, "b".getBytes());
    ConsumerRecord<byte[], byte[]> r105 =
        new ConsumerRecord<>("topic-a", 0, 105L, null, "c".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r100, r102, r105))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    // Ack 102 and 105 first; 100 is still in flight, so neither commit fires.
    consumer.acknowledge(messages.get(1));
    consumer.acknowledge(messages.get(2));
    verify(mockKafkaClient, never()).commitSync(any(Map.class));

    // Ack 100 — watermark advances over the entire delivered prefix
    // (100, 102, 105) and commits 106 in a single call.
    consumer.acknowledge(messages.get(0));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(106L))));
    verify(mockKafkaClient, org.mockito.Mockito.times(1)).commitSync(any(Map.class));
  }

  @Test
  void poll_seeksBackAndClearsBookkeeping_whenInFlightAtCap() {
    KafkaConsumerConfig cappedConfig =
        new KafkaConsumerConfig("broker:9092", "test-group", Map.of(), 2);
    KafkaConsumer cappedConsumer =
        new KafkaConsumer(cappedConfig, props -> mockKafkaClient);
    cappedConsumer.connect();

    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r10 =
        new ConsumerRecord<>("topic-a", 0, 10L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r11 =
        new ConsumerRecord<>("topic-a", 0, 11L, null, "b".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r10, r11))))
        .thenReturn(ConsumerRecords.empty());
    when(mockKafkaClient.assignment()).thenReturn(Set.of(tp));

    // Fill the cap: two records delivered, neither acked.
    List<Message> first = cappedConsumer.poll(Duration.ofMillis(10));
    assertEquals(2, first.size());

    // At-cap: poll() seeks back to the lowest unacked offset, clears
    // bookkeeping, and returns empty without calling the underlying poll().
    assertTrue(cappedConsumer.poll(Duration.ofMillis(10)).isEmpty());
    verify(mockKafkaClient).seek(tp, 10L);
    verify(mockKafkaClient, org.mockito.Mockito.times(1)).poll(any(Duration.class));

    // Bookkeeping was cleared, so the cleared Message can no longer be acked
    // (it will be redelivered on the next poll).
    assertThrows(IllegalStateException.class, () -> cappedConsumer.acknowledge(first.get(0)));

    // Next poll resumes calling the underlying client (cap no longer blocks).
    cappedConsumer.poll(Duration.ofMillis(10));
    verify(mockKafkaClient, org.mockito.Mockito.times(2)).poll(any(Duration.class));
  }

  @Test
  void poll_doesNotSeekPartitionsWithoutDeliveredRecords_whenAtCap() {
    // Only one partition has tracked records; the other is assigned but
    // empty. seek() must only fire for the partition with bookkeeping.
    KafkaConsumerConfig cappedConfig =
        new KafkaConsumerConfig("broker:9092", "test-group", Map.of(), 1);
    KafkaConsumer cappedConsumer =
        new KafkaConsumer(cappedConfig, props -> mockKafkaClient);
    cappedConsumer.connect();

    TopicPartition active = new TopicPartition("topic-a", 0);
    TopicPartition idle = new TopicPartition("topic-a", 1);
    ConsumerRecord<byte[], byte[]> r5 =
        new ConsumerRecord<>("topic-a", 0, 5L, null, "x".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(active, List.of(r5))));
    when(mockKafkaClient.assignment()).thenReturn(Set.of(active, idle));

    cappedConsumer.poll(Duration.ofMillis(10));
    // Now at cap; second poll should seek active to 5L but not seek idle.
    cappedConsumer.poll(Duration.ofMillis(10));

    verify(mockKafkaClient).seek(active, 5L);
    verify(mockKafkaClient, never()).seek(eq(idle), anyLong());
  }

  @Test
  void poll_uncapped_doesNotShortCircuit() {
    // Default config has maxInFlightMessages = 0 → no cap; poll() always
    // delegates to the underlying client even when many records are in flight.
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r10 =
        new ConsumerRecord<>("topic-a", 0, 10L, null, "x".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r10))))
        .thenReturn(ConsumerRecords.empty());

    consumer.poll(Duration.ofMillis(10));
    consumer.poll(Duration.ofMillis(10));
    verify(mockKafkaClient, org.mockito.Mockito.times(2)).poll(any(Duration.class));
  }

  @Test
  void acknowledge_inOrderCommitsAfterEachAck() {
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r10 =
        new ConsumerRecord<>("topic-a", 0, 10L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r11 =
        new ConsumerRecord<>("topic-a", 0, 11L, null, "b".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r10, r11))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(11L))));

    consumer.acknowledge(messages.get(1));
    verify(mockKafkaClient)
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(12L))));
  }

  @Test
  void acknowledge_failedAckCleanedUpWhenSuccessorAdvancesCommit() {
    // Two messages on the same partition: m1 @ offset 100, m2 @ offset 101.
    // acknowledge(m1) fails inside commitSync, leaving m1's partitionByMessageId /
    // offsetByMessageId entries in place (intact-for-retry semantic).
    // acknowledge(m2) then succeeds, committing past m1's offset via the watermark — without
    // the sweep, m1's id-keyed side-map entries would leak forever. A subsequent
    // acknowledge(m1) must therefore now throw "Unknown message" (proving the sweep cleaned
    // them up), instead of quietly succeeding and leaving the acked set with a stale entry.
    consumer.connect();
    TopicPartition tp = new TopicPartition("topic-a", 0);
    ConsumerRecord<byte[], byte[]> r100 =
        new ConsumerRecord<>("topic-a", 0, 100L, null, "a".getBytes());
    ConsumerRecord<byte[], byte[]> r101 =
        new ConsumerRecord<>("topic-a", 0, 101L, null, "b".getBytes());
    when(mockKafkaClient.poll(any(Duration.class)))
        .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(r100, r101))));

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    Message m1 = messages.get(0);
    Message m2 = messages.get(1);

    // First commitSync (from acknowledge(m1)) throws; second (from acknowledge(m2)) succeeds.
    doThrow(new CommitFailedException())
        .doNothing()
        .when(mockKafkaClient)
        .commitSync(any(Map.class));

    assertThrows(CommitFailedException.class, () -> consumer.acknowledge(m1));
    consumer.acknowledge(m2);

    // Commit advanced past both offsets (101 + 1).
    verify(mockKafkaClient, org.mockito.Mockito.times(1))
        .commitSync(eq(Collections.singletonMap(tp, new OffsetAndMetadata(102L))));

    // The successful commit's sweep must have cleaned m1's side-map entries even though m1's
    // own acknowledge threw. A re-ack of m1 therefore looks like an unknown message.
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(m1));
  }
}
