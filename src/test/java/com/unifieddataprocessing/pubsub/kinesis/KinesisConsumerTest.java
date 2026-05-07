package com.unifieddataprocessing.pubsub.kinesis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

@ExtendWith(MockitoExtension.class)
class KinesisConsumerTest {

  private static final String STREAM = "test-stream";

  @Mock private KinesisClient mockClient;

  private AtomicReference<KinesisConsumerConfig> capturedConfig;
  private KinesisConsumerConfig config;
  private KinesisConsumer consumer;

  @BeforeEach
  void setUp() {
    capturedConfig = new AtomicReference<>();
    // Throttling disabled by default so tests don't sleep between back-to-back
    // GetRecords calls; the dedicated throttle test sets a non-zero interval.
    config =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            Duration.ZERO);
    consumer =
        new KinesisConsumer(
            config,
            c -> {
              capturedConfig.set(c);
              return mockClient;
            });
  }

  @Test
  void connect_buildsClientFromFactoryAndCallsDescribeStreamSummary() {
    consumer.connect();

    assertSame(config, capturedConfig.get());
    ArgumentCaptor<DescribeStreamSummaryRequest> captor =
        ArgumentCaptor.forClass(DescribeStreamSummaryRequest.class);
    verify(mockClient).describeStreamSummary(captor.capture());
    assertEquals(STREAM, captor.getValue().streamName());
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    consumer.connect();
    assertThrows(IllegalStateException.class, consumer::connect);
  }

  @Test
  void connect_releasesClientWhenStreamValidationFails() {
    // If DescribeStreamSummary fails (missing stream, bad credentials,
    // transient error), connect() must close the SDK client and leave the
    // consumer disconnected so a retry doesn't see "already connected".
    when(mockClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
        .thenThrow(new RuntimeException("stream not found"));

    assertThrows(RuntimeException.class, consumer::connect);

    verify(mockClient).close();
    IllegalStateException notConnected =
        assertThrows(IllegalStateException.class, () -> consumer.subscribe(STREAM));
    assertTrue(notConnected.getMessage().contains("not connected"));
  }

  @Test
  void operationsBeforeConnect_throw() {
    assertThrows(IllegalStateException.class, () -> consumer.subscribe(STREAM));
    assertThrows(IllegalStateException.class, () -> consumer.unsubscribe(STREAM));
    assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    assertThrows(
        IllegalStateException.class,
        () -> consumer.acknowledge(new Message("id", STREAM, new byte[0], null)));
  }

  @Test
  void subscribe_rejectsTopicOtherThanConfiguredStream() {
    consumer.connect();
    assertThrows(IllegalArgumentException.class, () -> consumer.subscribe("some-other-stream"));
  }

  @Test
  void subscribe_listsAllShardsAndAcquiresIterators() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(
                    Shard.builder().shardId("shard-0").build(),
                    Shard.builder().shardId("shard-1").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    consumer.subscribe(STREAM);

    ArgumentCaptor<GetShardIteratorRequest> captor =
        ArgumentCaptor.forClass(GetShardIteratorRequest.class);
    verify(mockClient, times(2)).getShardIterator(captor.capture());
    List<GetShardIteratorRequest> requests = captor.getAllValues();
    assertEquals(STREAM, requests.get(0).streamName());
    assertEquals(ShardIteratorType.TRIM_HORIZON, requests.get(0).shardIteratorType());
    assertEquals(
        Map.of("shard-0", "ITER", "shard-1", "ITER"),
        Map.of(
            requests.get(0).shardId(),
            "ITER",
            requests.get(1).shardId(),
            "ITER"));
  }

  @Test
  void subscribe_paginatesListShards() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .nextToken("page-2")
                .build())
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-1").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    consumer.subscribe(STREAM);

    ArgumentCaptor<ListShardsRequest> captor = ArgumentCaptor.forClass(ListShardsRequest.class);
    verify(mockClient, times(2)).listShards(captor.capture());
    assertEquals(STREAM, captor.getAllValues().get(0).streamName());
    assertEquals("page-2", captor.getAllValues().get(1).nextToken());
    verify(mockClient, times(2)).getShardIterator(any(GetShardIteratorRequest.class));
  }

  @Test
  void subscribe_isIdempotent() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    consumer.subscribe(STREAM);
    consumer.subscribe(STREAM);

    verify(mockClient, times(1)).listShards(any(ListShardsRequest.class));
  }

  @Test
  void subscribe_atTimestampForwardsTimestamp() {
    KinesisConsumerConfig timestampConfig =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.atTimestamp(Instant.parse("2026-01-01T00:00:00Z")),
            100,
            Duration.ZERO);
    KinesisConsumer tsConsumer = new KinesisConsumer(timestampConfig, c -> mockClient);
    tsConsumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    tsConsumer.subscribe(STREAM);

    ArgumentCaptor<GetShardIteratorRequest> captor =
        ArgumentCaptor.forClass(GetShardIteratorRequest.class);
    verify(mockClient).getShardIterator(captor.capture());
    assertEquals(ShardIteratorType.AT_TIMESTAMP, captor.getValue().shardIteratorType());
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), captor.getValue().timestamp());
  }

  @Test
  void unsubscribe_clearsIteratorsAndIsIdempotent() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());
    consumer.subscribe(STREAM);

    consumer.unsubscribe(STREAM);
    // Second unsubscribe is a no-op; poll returns empty without hitting the SDK.
    consumer.unsubscribe(STREAM);

    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
    verify(mockClient, never()).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  void poll_emptyWhenNotSubscribed() {
    consumer.connect();
    assertTrue(consumer.poll(Duration.ofMillis(10)).isEmpty());
    verify(mockClient, never()).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  void poll_mapsRecordsToMessagesAndAdvancesIterator() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build());
    consumer.subscribe(STREAM);

    Instant arrived = Instant.parse("2026-05-01T12:00:00Z");
    Record record =
        Record.builder()
            .sequenceNumber("49600000000000000000000000000000000000000000000001")
            .partitionKey("pk-1")
            .data(SdkBytes.fromUtf8String("payload"))
            .approximateArrivalTimestamp(arrived)
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder().records(record).nextShardIterator("ITER-1").build());

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    assertEquals(1, messages.size());
    Message m = messages.get(0);
    assertEquals(
        "shard-0:49600000000000000000000000000000000000000000000001", m.getId());
    assertEquals(STREAM, m.getTopic());
    assertArrayEquals("payload".getBytes(), m.getPayload());
    assertEquals("pk-1", m.getAttributes().get(KinesisConsumer.ATTR_PARTITION_KEY));
    assertEquals(
        arrived.toString(),
        m.getAttributes().get(KinesisConsumer.ATTR_APPROXIMATE_ARRIVAL_TIMESTAMP));

    // Subsequent poll should use the new iterator from the prior response.
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(GetRecordsResponse.builder().nextShardIterator("ITER-2").build());
    consumer.poll(Duration.ofMillis(10));
    ArgumentCaptor<GetRecordsRequest> captor = ArgumentCaptor.forClass(GetRecordsRequest.class);
    verify(mockClient, times(2)).getRecords(captor.capture());
    assertEquals("ITER-0", captor.getAllValues().get(0).shardIterator());
    assertEquals(100, captor.getAllValues().get(0).limit());
    assertEquals("ITER-1", captor.getAllValues().get(1).shardIterator());
  }

  @Test
  void poll_dropsShardWhenNextShardIteratorIsNull() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build());
    consumer.subscribe(STREAM);

    // Closed shard: nextShardIterator null and no records.
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(GetRecordsResponse.builder().build());

    consumer.poll(Duration.ofMillis(10));

    // Second poll should not call getRecords again (shard was dropped).
    consumer.poll(Duration.ofMillis(10));
    verify(mockClient, times(1)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  void poll_resumesFromLowestUnackedSequenceOnExpiryWhenNoCheckpointYet() {
    // If the iterator expires after records were delivered but before any ack
    // has advanced the watermark, the reacquire must use the lowest unacked
    // sequence — not the configured starting position — so unacked records
    // are redelivered (at-least-once) rather than silently skipped.
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-INITIAL").build())
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-AFTER-EXPIRY").build());
    Record r100 =
        Record.builder()
            .sequenceNumber("100")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("a"))
            .build();
    Record r105 =
        Record.builder()
            .sequenceNumber("105")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("b"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder().records(r100, r105).nextShardIterator("ITER-1").build())
        .thenThrow(ExpiredIteratorException.builder().message("expired").build())
        .thenReturn(GetRecordsResponse.builder().nextShardIterator("ITER-2").build());
    consumer.subscribe(STREAM);

    // First poll delivers two records but no ack happens.
    consumer.poll(Duration.ofMillis(50));
    assertNull(consumer.getCheckpoints().get("shard-0"));

    // Second poll: GetRecords throws ExpiredIteratorException → reacquire.
    consumer.poll(Duration.ofMillis(50));

    ArgumentCaptor<GetShardIteratorRequest> captor =
        ArgumentCaptor.forClass(GetShardIteratorRequest.class);
    verify(mockClient, times(2)).getShardIterator(captor.capture());
    GetShardIteratorRequest reacquire = captor.getAllValues().get(1);
    assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, reacquire.shardIteratorType());
    assertEquals("100", reacquire.startingSequenceNumber());
  }

  @Test
  void acknowledge_tracksHighestSequenceNumberPerShard() {
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "100", "105");

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0));
    assertEquals("100", consumer.getCheckpoints().get("shard-0"));
    consumer.acknowledge(messages.get(1));
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void acknowledge_doesNotAdvanceCheckpointPastUnackedSequence() {
    // If acks arrive out of order, the checkpoint must NOT advance past an
    // unacked record. Otherwise persisting it and restarting with
    // afterSequenceNumber(...) would skip the in-flight record permanently.
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "100", "105");

    List<Message> messages = consumer.poll(Duration.ofMillis(10));

    // Ack "105" first while "100" is still in flight: no checkpoint yet.
    consumer.acknowledge(messages.get(1));
    assertNull(consumer.getCheckpoints().get("shard-0"));

    // Ack "100": prefix advances all the way to "105" in one step.
    consumer.acknowledge(messages.get(0));
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void acknowledge_usesBigIntegerComparisonForLexicographicSequenceNumbers() {
    // The lexicographic gotcha: as strings, "2" > "10"; as BigInteger, "10" > "2".
    // After both are acked, the checkpoint must reflect numeric (not string) order.
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "10", "2");

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0)); // ack "10"
    consumer.acknowledge(messages.get(1)); // ack "2"

    assertEquals("10", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void poll_sleepsForRemainingTimeoutWhenAllShardsEmpty() {
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build());
    consumer.subscribe(STREAM);
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(GetRecordsResponse.builder().nextShardIterator("ITER-1").build());

    long start = System.nanoTime();
    List<Message> result = consumer.poll(Duration.ofMillis(50));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(result.isEmpty());
    assertTrue(
        elapsedMs >= 45,
        "Expected poll() to sleep ~50ms when GetRecords returned empty, slept " + elapsedMs);
  }

  @Test
  void acknowledge_throwsForUnknownMessageId() {
    consumer.connect();
    Message stranger = new Message("not-from-poll", STREAM, new byte[0], null);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(stranger));
  }

  @Test
  void connect_resetsRetainedCheckpointsFromPriorSession() {
    // close() retains highWatermarkByShard so callers can persist it via
    // getCheckpoints(); the watermark must NOT bleed into a subsequent
    // connect() — a TRIM_HORIZON subscription that hits an expired iterator
    // would otherwise reacquire AFTER_SEQUENCE_NUMBER on the stale watermark
    // and skip records the new session should deliver.
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "100", "105");
    List<Message> messages = consumer.poll(Duration.ofMillis(50));
    consumer.acknowledge(messages.get(0));
    consumer.acknowledge(messages.get(1));
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));

    consumer.close();
    // Persistence path: getCheckpoints() still works after close.
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));

    consumer.connect();
    assertNull(consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void acknowledge_keepsCheckpointMonotonicAcrossUnsubscribeResubscribe() {
    // After acking through seq=105, an unsubscribe+resubscribe with a
    // re-delivery of older records (e.g. TRIM_HORIZON) must NOT regress the
    // checkpoint — acking a smaller delivered prefix on the new session
    // should leave highWatermarkByShard at the prior maximum.
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "100", "105");
    List<Message> first = consumer.poll(Duration.ofMillis(50));
    consumer.acknowledge(first.get(0));
    consumer.acknowledge(first.get(1));
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));

    // Unsubscribe and re-subscribe; iterators are reacquired and the same
    // records are delivered again (same id format → re-enters side-maps).
    consumer.unsubscribe(STREAM);
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-RESUBSCRIBE").build());
    Record r100 =
        Record.builder()
            .sequenceNumber("100")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("a"))
            .build();
    Record r105 =
        Record.builder()
            .sequenceNumber("105")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("b"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder()
                .records(r100, r105)
                .nextShardIterator("ITER-NEXT")
                .build());
    consumer.subscribe(STREAM);
    List<Message> second = consumer.poll(Duration.ofMillis(50));
    assertEquals(2, second.size());

    // Ack just the lower seq on the new session — the checkpoint must stay
    // at the prior max ("105"), not regress to "100".
    consumer.acknowledge(second.get(0));
    assertEquals("105", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void close_isIdempotentAndClosesUnderlyingClient() {
    consumer.connect();
    consumer.close();
    consumer.close();
    verify(mockClient, times(1)).close();
  }

  @Test
  void getCheckpoints_returnsUnmodifiableSnapshot() {
    consumer.connect();
    seedTwoMessagesOnShard("shard-0", "100", "200");
    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0));

    Map<String, String> snapshot = consumer.getCheckpoints();
    assertEquals("100", snapshot.get("shard-0"));
    assertThrows(UnsupportedOperationException.class, () -> snapshot.put("x", "y"));

    // A later ack should not be visible in the prior snapshot.
    consumer.acknowledge(messages.get(1));
    assertEquals("100", snapshot.get("shard-0"));
    assertEquals("200", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void subscribe_filtersListShardsToOpenWhenStartingAtLatest() {
    // With LATEST, we don't want iterators for closed historical shards
    // (they'd just cost a GetShardIterator + GetRecords roundtrip and get
    // immediately dropped). Pass ShardFilter(AT_LATEST) so Kinesis only
    // returns currently-open shards.
    KinesisConsumerConfig latestConfig =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.latest(),
            100,
            Duration.ZERO);
    KinesisConsumer latestConsumer = new KinesisConsumer(latestConfig, c -> mockClient);
    latestConsumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    latestConsumer.subscribe(STREAM);

    ArgumentCaptor<ListShardsRequest> captor = ArgumentCaptor.forClass(ListShardsRequest.class);
    verify(mockClient).listShards(captor.capture());
    ListShardsRequest req = captor.getValue();
    assertNotNull(req.shardFilter());
    assertEquals(ShardFilterType.AT_LATEST, req.shardFilter().type());
  }

  @Test
  void subscribe_filtersListShardsToTimestampWhenStartingAtTimestamp() {
    // Use FROM_TIMESTAMP rather than AT_TIMESTAMP so a stream resharded
    // after the configured time still surfaces its current shards
    // (AT_TIMESTAMP would only return shards open at that exact moment, and
    // we don't dynamically discover children of closed shards).
    Instant ts = Instant.parse("2026-01-01T00:00:00Z");
    KinesisConsumerConfig tsConfig =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.atTimestamp(ts),
            100,
            Duration.ZERO);
    KinesisConsumer tsConsumer = new KinesisConsumer(tsConfig, c -> mockClient);
    tsConsumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    tsConsumer.subscribe(STREAM);

    ArgumentCaptor<ListShardsRequest> captor = ArgumentCaptor.forClass(ListShardsRequest.class);
    verify(mockClient).listShards(captor.capture());
    ListShardsRequest req = captor.getValue();
    assertNotNull(req.shardFilter());
    assertEquals(ShardFilterType.FROM_TIMESTAMP, req.shardFilter().type());
    assertEquals(ts, req.shardFilter().timestamp());
  }

  @Test
  void subscribe_doesNotFilterListShardsForTrimHorizon() {
    // setUp uses TRIM_HORIZON; the default Kinesis behavior already matches.
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    consumer.subscribe(STREAM);

    ArgumentCaptor<ListShardsRequest> captor = ArgumentCaptor.forClass(ListShardsRequest.class);
    verify(mockClient).listShards(captor.capture());
    assertNull(captor.getValue().shardFilter());
  }

  @Test
  void subscribe_appliesPerShardStartingPositionOverrides() {
    // Round-trip restore: caller persisted a Map<shardId, sequenceNumber> from a
    // prior getCheckpoints(), and feeds it back via startingPositionByShard.
    // Each shard must get a GetShardIterator call with its own AFTER_SEQUENCE_NUMBER;
    // shards not present in the map fall back to the global startingPosition.
    KinesisConsumerConfig restored =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            Duration.ZERO,
            Map.of(
                "shard-0", KinesisStartingPosition.afterSequenceNumber("100"),
                "shard-1", KinesisStartingPosition.afterSequenceNumber("200")));
    KinesisConsumer restoredConsumer = new KinesisConsumer(restored, c -> mockClient);
    restoredConsumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(
                    Shard.builder().shardId("shard-0").build(),
                    Shard.builder().shardId("shard-1").build(),
                    Shard.builder().shardId("shard-2").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    restoredConsumer.subscribe(STREAM);

    ArgumentCaptor<GetShardIteratorRequest> captor =
        ArgumentCaptor.forClass(GetShardIteratorRequest.class);
    verify(mockClient, times(3)).getShardIterator(captor.capture());
    Map<String, GetShardIteratorRequest> byShard = new java.util.HashMap<>();
    captor.getAllValues().forEach(r -> byShard.put(r.shardId(), r));

    assertEquals(
        ShardIteratorType.AFTER_SEQUENCE_NUMBER,
        byShard.get("shard-0").shardIteratorType());
    assertEquals("100", byShard.get("shard-0").startingSequenceNumber());

    assertEquals(
        ShardIteratorType.AFTER_SEQUENCE_NUMBER,
        byShard.get("shard-1").shardIteratorType());
    assertEquals("200", byShard.get("shard-1").startingSequenceNumber());

    // shard-2 is not in the override map → global default (TRIM_HORIZON).
    assertEquals(ShardIteratorType.TRIM_HORIZON, byShard.get("shard-2").shardIteratorType());
    assertNull(byShard.get("shard-2").startingSequenceNumber());
  }

  @Test
  void poll_recoversFromExpiredShardIterator() {
    // Simulate >5 min of idleness: GetRecords throws ExpiredIteratorException.
    // The consumer must reacquire a fresh iterator from the latest checkpoint
    // (or the configured starting position if none) and continue without
    // requiring the caller to recreate the consumer.
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-INITIAL").build())
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-AFTER-EXPIRY").build());
    Record record =
        Record.builder()
            .sequenceNumber("100")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("a"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder().records(record).nextShardIterator("ITER-1").build())
        .thenThrow(ExpiredIteratorException.builder().message("expired").build())
        .thenReturn(GetRecordsResponse.builder().nextShardIterator("ITER-2").build());
    consumer.subscribe(STREAM);

    // First poll succeeds; ack to advance the watermark to 100.
    List<Message> first = consumer.poll(Duration.ofMillis(50));
    consumer.acknowledge(first.get(0));
    assertEquals("100", consumer.getCheckpoints().get("shard-0"));

    // Second poll: GetRecords throws ExpiredIteratorException → reacquire and skip.
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty());

    // Third poll uses the new iterator without throwing.
    consumer.poll(Duration.ofMillis(50));

    // Verify the reacquire used AFTER_SEQUENCE_NUMBER with the latest watermark.
    ArgumentCaptor<GetShardIteratorRequest> captor =
        ArgumentCaptor.forClass(GetShardIteratorRequest.class);
    verify(mockClient, times(2)).getShardIterator(captor.capture());
    GetShardIteratorRequest reacquire = captor.getAllValues().get(1);
    assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, reacquire.shardIteratorType());
    assertEquals("100", reacquire.startingSequenceNumber());
  }

  @Test
  void subscribe_doesNotPartiallyInitializeOnShardIteratorFailure() {
    // If acquireShardIterator throws for any shard after others succeeded,
    // no consumer state should be mutated — a retry must start from scratch
    // and initialize all shards rather than silently skipping the failed one.
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(
                    Shard.builder().shardId("shard-0").build(),
                    Shard.builder().shardId("shard-1").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build())
        .thenThrow(new RuntimeException("transient AWS error"))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0-retry").build())
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-1-retry").build());

    // First attempt fails midway through shard iterator acquisition.
    assertThrows(RuntimeException.class, () -> consumer.subscribe(STREAM));

    // No partial state: a poll should not call GetRecords for any shard,
    // because nothing was added to iteratorByShard.
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty());
    verify(mockClient, never()).getRecords(any(GetRecordsRequest.class));

    // Retry succeeds — both shards now subscribed; the next poll fetches both.
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(GetRecordsResponse.builder().nextShardIterator("X").build());
    consumer.subscribe(STREAM);
    consumer.poll(Duration.ofMillis(50));
    verify(mockClient, times(2)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  void config_rejectsInvalidGetRecordsLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new KinesisConsumerConfig(
                STREAM,
                Region.US_EAST_1,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
                KinesisStartingPosition.latest(),
                0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new KinesisConsumerConfig(
                STREAM,
                Region.US_EAST_1,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
                KinesisStartingPosition.latest(),
                10_001));
  }

  @Test
  void config_acceptsKinesisMaximumGetRecordsLimit() {
    new KinesisConsumerConfig(
        STREAM,
        Region.US_EAST_1,
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
        KinesisStartingPosition.latest(),
        10_000);
  }

  @Test
  void config_rejectsNegativeGetRecordsMinInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new KinesisConsumerConfig(
                STREAM,
                Region.US_EAST_1,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
                KinesisStartingPosition.latest(),
                100,
                Duration.ofMillis(-1)));
  }

  @Test
  void poll_throttlesGetRecordsToConfiguredMinIntervalPerShard() {
    // With min-interval = 100ms, two back-to-back successful polls on the same
    // shard must space the underlying GetRecords calls at least ~100ms apart
    // so we don't trip Kinesis's 5 TPS-per-shard cap.
    KinesisConsumerConfig throttledConfig =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            Duration.ofMillis(100));
    KinesisConsumer throttled = new KinesisConsumer(throttledConfig, c -> mockClient);
    throttled.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());
    Record record =
        Record.builder()
            .sequenceNumber("1")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("x"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder().records(record).nextShardIterator("ITER-N").build());
    throttled.subscribe(STREAM);

    long start = System.nanoTime();
    throttled.poll(Duration.ofMillis(500));
    throttled.poll(Duration.ofMillis(500));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(mockClient, times(2)).getRecords(any(GetRecordsRequest.class));
    assertTrue(
        elapsedMs >= 90,
        "Expected ~100ms throttle between GetRecords on the same shard, observed " + elapsedMs);
  }

  @Test
  void poll_throttlesByBytesAfterLargeResponse() {
    // Kinesis caps each shard at 2 MiB/s for GetRecords. After a large
    // response, the byte budget can require a longer wait than the TPS
    // budget; honoring only TPS would still trip ProvisionedThroughputExceededException.
    KinesisConsumerConfig byteThrottled =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            // Deliberately small TPS interval so the byte limit dominates.
            Duration.ofMillis(10));
    KinesisConsumer throttled = new KinesisConsumer(byteThrottled, c -> mockClient);
    throttled.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());

    // 500 KiB payload → 500 KiB / 2 MiB/s = 250 ms minimum gap.
    byte[] largePayload = new byte[500 * 1024];
    Record largeRecord =
        Record.builder()
            .sequenceNumber("1")
            .partitionKey("pk")
            .data(SdkBytes.fromByteArray(largePayload))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder()
                .records(largeRecord)
                .nextShardIterator("ITER-N")
                .build());
    throttled.subscribe(STREAM);

    long start = System.nanoTime();
    throttled.poll(Duration.ofMillis(1000));
    throttled.poll(Duration.ofMillis(1000));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(mockClient, times(2)).getRecords(any(GetRecordsRequest.class));
    assertTrue(
        elapsedMs >= 220,
        "Expected ~250ms byte-based throttle after a 500 KiB response, observed " + elapsedMs);
  }

  @Test
  void poll_respectsExhaustedDeadlineBeforeIssuingGetRecords() {
    // Polling with a zero (or already-past) deadline must NOT issue any
    // GetRecords on subscribed shards — synchronous SDK calls take time and
    // would blow past the caller's stated timeout. (The setup uses
    // Duration.ZERO for the throttle, so this exercises the deadline path
    // independent of the throttle logic.)
    consumer.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());
    consumer.subscribe(STREAM);

    assertTrue(consumer.poll(Duration.ZERO).isEmpty());
    verify(mockClient, never()).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  void poll_continuesPastThrottledShardToServiceReadyOnes() throws InterruptedException {
    // After both shards fetch in the first poll, shard-0 is throttled by the
    // byte budget (~250 ms for 500 KiB) and shard-1 only by the small TPS
    // interval. Once shard-1's throttle expires but shard-0's hasn't, a poll
    // within shard-0's remaining throttle must still service shard-1 — the
    // throttled shard cannot block ready shards.
    KinesisConsumerConfig cfg =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            Duration.ofMillis(50));
    KinesisConsumer multi = new KinesisConsumer(cfg, c -> mockClient);
    multi.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(
                    Shard.builder().shardId("shard-0").build(),
                    Shard.builder().shardId("shard-1").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build())
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-1").build());

    // shard-0's first response is large (~250ms byte throttle); shard-1's is small.
    Record largeRecord =
        Record.builder()
            .sequenceNumber("100")
            .partitionKey("pk")
            .data(SdkBytes.fromByteArray(new byte[500 * 1024]))
            .build();
    Record smallRecord =
        Record.builder()
            .sequenceNumber("200")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("x"))
            .build();
    Record smallRecord2 =
        Record.builder()
            .sequenceNumber("201")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("y"))
            .build();
    when(mockClient.getRecords(
            argThat(
                (GetRecordsRequest req) ->
                    req != null
                        && req.shardIterator() != null
                        && req.shardIterator().startsWith("ITER-0"))))
        .thenReturn(
            GetRecordsResponse.builder()
                .records(largeRecord)
                .nextShardIterator("ITER-0-NEXT")
                .build());
    when(mockClient.getRecords(
            argThat(
                (GetRecordsRequest req) ->
                    req != null
                        && req.shardIterator() != null
                        && req.shardIterator().startsWith("ITER-1"))))
        .thenReturn(
            GetRecordsResponse.builder()
                .records(smallRecord)
                .nextShardIterator("ITER-1-NEXT")
                .build())
        .thenReturn(
            GetRecordsResponse.builder()
                .records(smallRecord2)
                .nextShardIterator("ITER-1-NEXT-NEXT")
                .build());

    multi.subscribe(STREAM);

    // First poll: both shards fetch.
    List<Message> first = multi.poll(Duration.ofMillis(100));
    assertEquals(2, first.size());

    // Wait past shard-1's TPS throttle (~50 ms) but within shard-0's byte
    // throttle (~250 ms remaining ≈ 170 ms after this sleep).
    Thread.sleep(80);

    // Second poll with a 30 ms budget. shard-0 still throttled; shard-1 ready.
    // With break, we'd wait the full budget on shard-0 and never see shard-1.
    // With continue, shard-1 gets serviced regardless of iteration order.
    List<Message> second = multi.poll(Duration.ofMillis(30));
    assertEquals(1, second.size());
    assertTrue(
        second.get(0).getId().startsWith("shard-1:"),
        "Expected the ready shard-1 to be polled, got " + second.get(0).getId());
  }

  @Test
  void poll_skipsShardWhenThrottleExceedsRemainingTimeout() {
    // When the per-shard throttle would push past the caller's poll timeout,
    // poll() must return promptly without an extra GetRecords call (otherwise
    // we'd silently exceed the deadline).
    KinesisConsumerConfig throttledConfig =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100,
            Duration.ofSeconds(1));
    KinesisConsumer throttled = new KinesisConsumer(throttledConfig, c -> mockClient);
    throttled.connect();
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId("shard-0").build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER").build());
    Record record =
        Record.builder()
            .sequenceNumber("1")
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("x"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder().records(record).nextShardIterator("ITER-N").build());
    throttled.subscribe(STREAM);

    throttled.poll(Duration.ofMillis(50));
    // Second poll's budget (50ms) can't cover the 1s throttle gap → bails out.
    throttled.poll(Duration.ofMillis(50));

    verify(mockClient, times(1)).getRecords(any(GetRecordsRequest.class));
  }

  /**
   * Stubs ListShards/GetShardIterator/GetRecords so a single subscribe+poll yields two records on
   * the named shard with the given sequence numbers.
   */
  private void seedTwoMessagesOnShard(String shardId, String seq1, String seq2) {
    when(mockClient.listShards(any(ListShardsRequest.class)))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(Shard.builder().shardId(shardId).build())
                .build());
    when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator("ITER-0").build());
    Record r1 =
        Record.builder()
            .sequenceNumber(seq1)
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("a"))
            .build();
    Record r2 =
        Record.builder()
            .sequenceNumber(seq2)
            .partitionKey("pk")
            .data(SdkBytes.fromUtf8String("b"))
            .build();
    when(mockClient.getRecords(any(GetRecordsRequest.class)))
        .thenReturn(
            GetRecordsResponse.builder()
                .records(r1, r2)
                .nextShardIterator("ITER-1")
                .build());
    consumer.subscribe(STREAM);
  }
}
