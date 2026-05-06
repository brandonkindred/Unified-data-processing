package com.unifieddataprocessing.pubsub.kinesis;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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

/**
 * Amazon Kinesis Data Streams-backed {@link PubSubConsumer}. Wraps a synchronous {@link
 * KinesisClient} and consumes all shards of one stream in-process via {@code GetShardIterator}
 * + {@code GetRecords}.
 *
 * <p>This is the raw-SDK first cut: shard discovery happens once on the first {@link
 * #subscribe(String)}, with no resharding/lease management. Acknowledged sequence numbers are
 * tracked in memory (per shard) and exposed via {@link #getCheckpoints()} so the caller can
 * persist them — there is no broker-side committed offset to commit to. For multi-worker
 * production deployments use a KCL-based implementation instead.
 *
 * <p>Bound to the single stream {@link KinesisConsumerConfig#getStreamName()}: {@link
 * #subscribe(String)} accepts only that name. Multi-stream support is intentionally out of
 * scope.
 *
 * <p>Not thread-safe.
 */
public class KinesisConsumer implements PubSubConsumer {

  /** {@link Message} attribute key carrying the Kinesis record's partition key. */
  public static final String ATTR_PARTITION_KEY = "partitionKey";

  /** {@link Message} attribute key carrying the record's approximate arrival timestamp. */
  public static final String ATTR_APPROXIMATE_ARRIVAL_TIMESTAMP = "approximateArrivalTimestamp";

  private final KinesisConsumerConfig config;
  private final Function<KinesisConsumerConfig, KinesisClient> clientFactory;
  private final Set<String> subscribedTopics = new LinkedHashSet<>();
  // Active shard iterators keyed by shard id. Updated each poll from
  // GetRecords#nextShardIterator; a null nextShardIterator means the shard is
  // closed (resharding) so the entry is removed.
  private final Map<String, String> iteratorByShard = new HashMap<>();
  // Side-map: Message.id ("shardId:sequenceNumber") → (shardId, sequenceNumber).
  private final Map<String, ShardSequence> shardSeqByMessageId = new HashMap<>();
  // In-memory checkpoint: shardId → highest acked sequence number. Kinesis
  // sequence numbers are lexicographic numeric strings, so comparisons must
  // use BigInteger.compareTo (not String#compareTo).
  private final Map<String, BigInteger> highWatermarkByShard = new HashMap<>();

  private KinesisClient client;

  /** Creates a consumer that builds a real {@link KinesisClient} on {@link #connect()}. */
  public KinesisConsumer(KinesisConsumerConfig config) {
    this(
        config,
        c ->
            KinesisClient.builder()
                .region(c.getRegion())
                .credentialsProvider(c.getCredentialsProvider())
                .build());
  }

  KinesisConsumer(
      KinesisConsumerConfig config,
      Function<KinesisConsumerConfig, KinesisClient> clientFactory) {
    this.config = Objects.requireNonNull(config, "config");
    this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
  }

  @Override
  public void connect() {
    if (client != null) {
      throw new IllegalStateException("already connected");
    }
    client = clientFactory.apply(config);
    // Surface stream-not-found / auth errors at connect time.
    client.describeStreamSummary(
        DescribeStreamSummaryRequest.builder().streamName(config.getStreamName()).build());
  }

  @Override
  public void subscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    if (!topic.equals(config.getStreamName())) {
      throw new IllegalArgumentException(
          "this consumer is bound to stream '"
              + config.getStreamName()
              + "'; multi-stream subscription is not supported (got '"
              + topic
              + "')");
    }
    if (!subscribedTopics.add(topic)) {
      return;
    }
    for (Shard shard : listAllShards()) {
      iteratorByShard.put(shard.shardId(), acquireShardIterator(shard.shardId()));
    }
  }

  @Override
  public void unsubscribe(String topic) {
    Objects.requireNonNull(topic, "topic");
    ensureConnected();
    if (!subscribedTopics.remove(topic)) {
      return;
    }
    iteratorByShard.clear();
  }

  @Override
  public List<Message> poll(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    ensureConnected();
    if (iteratorByShard.isEmpty()) {
      return Collections.emptyList();
    }
    List<Message> result = new ArrayList<>();
    // Snapshot the keys; we may remove closed shards mid-iteration.
    List<String> shardIds = new ArrayList<>(iteratorByShard.keySet());
    for (String shardId : shardIds) {
      String iterator = iteratorByShard.get(shardId);
      GetRecordsResponse response =
          client.getRecords(
              GetRecordsRequest.builder()
                  .shardIterator(iterator)
                  .limit(config.getGetRecordsLimit())
                  .build());
      for (Record record : response.records()) {
        String seq = record.sequenceNumber();
        String id = shardId + ":" + seq;
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(ATTR_PARTITION_KEY, record.partitionKey());
        if (record.approximateArrivalTimestamp() != null) {
          attributes.put(
              ATTR_APPROXIMATE_ARRIVAL_TIMESTAMP,
              record.approximateArrivalTimestamp().toString());
        }
        byte[] payload = record.data() == null ? new byte[0] : record.data().asByteArray();
        shardSeqByMessageId.put(id, new ShardSequence(shardId, seq));
        result.add(new Message(id, config.getStreamName(), payload, attributes));
      }
      if (response.nextShardIterator() == null) {
        // Shard is closed (split/merge). A KCL-based implementation would
        // discover and start its children here; this raw-SDK cut just stops.
        iteratorByShard.remove(shardId);
      } else {
        iteratorByShard.put(shardId, response.nextShardIterator());
      }
    }
    return result;
  }

  @Override
  public void acknowledge(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    ShardSequence ss = shardSeqByMessageId.get(message.getId());
    if (ss == null) {
      throw new IllegalStateException(
          "Unknown message: "
              + message.getId()
              + ". Only messages returned by poll() can be acknowledged.");
    }
    BigInteger seq = new BigInteger(ss.sequenceNumber());
    BigInteger current = highWatermarkByShard.get(ss.shardId());
    if (current == null || seq.compareTo(current) > 0) {
      highWatermarkByShard.put(ss.shardId(), seq);
    }
    shardSeqByMessageId.remove(message.getId());
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } finally {
      client = null;
      subscribedTopics.clear();
      iteratorByShard.clear();
      shardSeqByMessageId.clear();
      // highWatermarkByShard intentionally retained so getCheckpoints() can be
      // read post-close to persist final state.
    }
  }

  /**
   * Snapshot of in-memory checkpoints: shard id → highest acknowledged sequence number. The
   * caller is responsible for persisting these across process restarts; this consumer does not.
   */
  public Map<String, String> getCheckpoints() {
    Map<String, String> snapshot = new LinkedHashMap<>();
    for (Map.Entry<String, BigInteger> e : highWatermarkByShard.entrySet()) {
      snapshot.put(e.getKey(), e.getValue().toString());
    }
    return Collections.unmodifiableMap(snapshot);
  }

  private List<Shard> listAllShards() {
    List<Shard> shards = new ArrayList<>();
    String nextToken = null;
    do {
      ListShardsRequest.Builder req = ListShardsRequest.builder();
      if (nextToken == null) {
        req.streamName(config.getStreamName());
      } else {
        req.nextToken(nextToken);
      }
      ListShardsResponse resp = client.listShards(req.build());
      shards.addAll(resp.shards());
      nextToken = resp.nextToken();
    } while (nextToken != null);
    return shards;
  }

  private String acquireShardIterator(String shardId) {
    KinesisStartingPosition pos = config.getStartingPosition();
    GetShardIteratorRequest.Builder req =
        GetShardIteratorRequest.builder()
            .streamName(config.getStreamName())
            .shardId(shardId)
            .shardIteratorType(pos.getType());
    if (pos.getTimestamp() != null) {
      req.timestamp(pos.getTimestamp());
    }
    if (pos.getSequenceNumber() != null) {
      req.startingSequenceNumber(pos.getSequenceNumber());
    }
    GetShardIteratorResponse resp = client.getShardIterator(req.build());
    return resp.shardIterator();
  }

  private void ensureConnected() {
    if (client == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }

  private record ShardSequence(String shardId, String sequenceNumber) {}
}
