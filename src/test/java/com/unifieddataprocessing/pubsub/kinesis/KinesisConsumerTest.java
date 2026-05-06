package com.unifieddataprocessing.pubsub.kinesis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
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
    config =
        new KinesisConsumerConfig(
            STREAM,
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")),
            KinesisStartingPosition.trimHorizon(),
            100);
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
            100);
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
  void acknowledge_outOfOrderDoesNotRegressWatermark() {
    consumer.connect();
    // The lexicographic gotcha: as strings, "2" > "10". With BigInteger, "10" > "2".
    seedTwoMessagesOnShard("shard-0", "10", "2");

    List<Message> messages = consumer.poll(Duration.ofMillis(10));
    consumer.acknowledge(messages.get(0)); // ack "10" first
    consumer.acknowledge(messages.get(1)); // then ack "2" — must NOT replace watermark

    assertEquals("10", consumer.getCheckpoints().get("shard-0"));
  }

  @Test
  void acknowledge_throwsForUnknownMessageId() {
    consumer.connect();
    Message stranger = new Message("not-from-poll", STREAM, new byte[0], null);
    assertThrows(IllegalStateException.class, () -> consumer.acknowledge(stranger));
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
                10_000));
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
