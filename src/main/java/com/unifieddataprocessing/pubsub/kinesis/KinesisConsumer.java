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
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

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

  /**
   * Kinesis's documented per-shard read budget for {@code GetRecords} (2 MiB/sec). Used to
   * back off when a single fetch returns more bytes than the next 200 ms can absorb.
   */
  private static final long READ_BYTES_PER_SECOND_PER_SHARD = 2L * 1024L * 1024L;

  private final KinesisConsumerConfig config;
  private final Function<KinesisConsumerConfig, KinesisClient> clientFactory;
  private final long getRecordsMinIntervalNanos;
  private final Set<String> subscribedTopics = new LinkedHashSet<>();
  // Active shard iterators keyed by shard id. Updated each poll from
  // GetRecords#nextShardIterator; a null nextShardIterator means the shard is
  // closed (resharding) so the entry is removed. LinkedHashMap so poll()
  // visits shards in subscribe order — deterministic and fair across calls.
  private final Map<String, String> iteratorByShard = new LinkedHashMap<>();
  // Per-shard wall-clock (System.nanoTime) at which the next GetRecords call is
  // allowed. Updated after every fetch to the later of the TPS limit
  // (now + getRecordsMinInterval) and the byte limit
  // (now + bytes-just-read / 2 MiB-per-sec) — Kinesis enforces both per shard.
  private final Map<String, Long> nextAllowedFetchNanosByShard = new HashMap<>();
  // Side-map: Message.id ("shardId:sequenceNumber") → (shardId, sequenceNumber).
  private final Map<String, ShardSequence> shardSeqByMessageId = new HashMap<>();
  // Per-shard delivered and acked sequence numbers, sorted by BigInteger
  // natural order. The checkpoint advances only across the longest fully-acked
  // prefix of delivered, so an out-of-order ack on a higher seq cannot skip
  // earlier in-flight records on the same shard. Mirrors KafkaConsumer's
  // gap-tolerant watermark logic.
  private final Map<String, NavigableSet<BigInteger>> deliveredSeqsByShard = new HashMap<>();
  private final Map<String, NavigableSet<BigInteger>> ackedSeqsByShard = new HashMap<>();
  // In-memory checkpoint: shardId → highest contiguously-acked sequence number.
  // Kinesis sequence numbers are lexicographic numeric strings, so comparisons
  // must use BigInteger.compareTo (not String#compareTo).
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
    this.getRecordsMinIntervalNanos = config.getGetRecordsMinInterval().toNanos();
  }

  @Override
  public void connect() {
    if (client != null) {
      throw new IllegalStateException("already connected");
    }
    // Build the client locally and validate before publishing the reference,
    // so a failed DescribeStreamSummary (missing stream, bad credentials,
    // transient AWS error) leaves the consumer cleanly disconnected and
    // releases the SDK client instead of leaking it.
    KinesisClient newClient = clientFactory.apply(config);
    try {
      newClient.describeStreamSummary(
          DescribeStreamSummaryRequest.builder().streamName(config.getStreamName()).build());
    } catch (RuntimeException e) {
      try {
        newClient.close();
      } catch (RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    client = newClient;
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
    if (subscribedTopics.contains(topic)) {
      return;
    }
    // Acquire every shard iterator into a local map before mutating consumer
    // state. If listAllShards or any acquireShardIterator call throws (e.g.
    // a transient AWS error on shard N), the consumer stays cleanly
    // unsubscribed so a retry can start from scratch and initialize all
    // shards — avoiding a half-subscribed state where subsequent polls
    // would silently see only a partial set of shards.
    List<Shard> shards = listAllShards();
    Map<String, String> newIterators = new LinkedHashMap<>();
    for (Shard shard : shards) {
      newIterators.put(
          shard.shardId(),
          acquireShardIterator(shard.shardId(), startingPositionFor(shard.shardId())));
    }
    iteratorByShard.putAll(newIterators);
    subscribedTopics.add(topic);
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
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    List<Message> result = new ArrayList<>();
    // Snapshot the keys; we may remove closed shards mid-iteration.
    List<String> shardIds = new ArrayList<>(iteratorByShard.keySet());
    for (String shardId : shardIds) {
      // Per-shard rate limit: Kinesis caps GetRecords at 5 TPS *and* 2 MiB/s
      // per shard. Sleep until both budgets have been replenished from the
      // last call. Cap the sleep at the remaining poll budget so we don't
      // exceed the caller's timeout. If this shard's throttle outlasts the
      // budget, skip it (continue) so a ready shard later in the iteration
      // order isn't blocked behind a throttled one.
      if (!awaitNextFetch(shardId, deadlineNanos)) {
        continue;
      }
      String iterator = iteratorByShard.get(shardId);
      GetRecordsResponse response;
      try {
        response =
            client.getRecords(
                GetRecordsRequest.builder()
                    .shardIterator(iterator)
                    .limit(config.getGetRecordsLimit())
                    .build());
      } catch (ExpiredIteratorException e) {
        // Iterators expire after 5 minutes of idleness. Reacquire from the
        // highest acked sequence (so we don't redeliver acked records) or
        // the configured starting position if no progress has been made,
        // and skip this shard for the rest of this poll — the next poll
        // will use the fresh iterator. May redeliver delivered-but-unacked
        // records, which is consistent with the framework's at-least-once
        // semantics. Still throttle (TPS only, zero bytes consumed) so a
        // pathological retry loop can't slip the per-shard call cap.
        iteratorByShard.put(shardId, acquireShardIterator(shardId, resumePositionFor(shardId)));
        recordFetch(shardId, 0L);
        continue;
      }
      long fetchedBytes = 0L;
      for (Record record : response.records()) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(ATTR_PARTITION_KEY, record.partitionKey());
        if (record.approximateArrivalTimestamp() != null) {
          attributes.put(
              ATTR_APPROXIMATE_ARRIVAL_TIMESTAMP,
              record.approximateArrivalTimestamp().toString());
        }
        byte[] payload = record.data() == null ? new byte[0] : record.data().asByteArray();
        fetchedBytes += payload.length;
        String seq = record.sequenceNumber();
        String id = shardId + ":" + seq;
        shardSeqByMessageId.put(id, new ShardSequence(shardId, seq));
        deliveredSeqsByShard
            .computeIfAbsent(shardId, k -> new TreeSet<>())
            .add(new BigInteger(seq));
        result.add(new Message(id, config.getStreamName(), payload, attributes));
      }
      recordFetch(shardId, fetchedBytes);
      if (response.nextShardIterator() == null) {
        // Shard is closed (split/merge). A KCL-based implementation would
        // discover and start its children here; this raw-SDK cut just stops.
        iteratorByShard.remove(shardId);
        nextAllowedFetchNanosByShard.remove(shardId);
      } else {
        iteratorByShard.put(shardId, response.nextShardIterator());
      }
    }
    // GetRecords doesn't long-poll, so an idle stream returns immediately.
    // Block out the remainder of the caller's timeout to avoid spin-loops in
    // the empty case (matches the blocking semantics callers get from Kafka).
    if (result.isEmpty()) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos > 0) {
        try {
          TimeUnit.NANOSECONDS.sleep(remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
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
    NavigableSet<BigInteger> delivered =
        deliveredSeqsByShard.computeIfAbsent(ss.shardId(), k -> new TreeSet<>());
    NavigableSet<BigInteger> acked =
        ackedSeqsByShard.computeIfAbsent(ss.shardId(), k -> new TreeSet<>());
    acked.add(seq);

    // Walk delivered seqs in order; the highest fully-acked prefix sets the
    // checkpoint. This ensures getCheckpoints() never advances past an
    // unacked record on the same shard — persisting the watermark and
    // restarting with afterSequenceNumber(...) is safe.
    BigInteger highestPrefix = null;
    for (BigInteger d : delivered) {
      if (acked.contains(d)) {
        highestPrefix = d;
      } else {
        break;
      }
    }
    if (highestPrefix != null) {
      highWatermarkByShard.put(ss.shardId(), highestPrefix);
      delivered.headSet(highestPrefix, true).clear();
      acked.headSet(highestPrefix, true).clear();
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
      nextAllowedFetchNanosByShard.clear();
      shardSeqByMessageId.clear();
      deliveredSeqsByShard.clear();
      ackedSeqsByShard.clear();
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
    ShardFilter filter = buildShardFilter();
    do {
      ListShardsRequest.Builder req = ListShardsRequest.builder();
      if (nextToken == null) {
        // First page: scope by stream name and (optionally) a shard filter so
        // we don't acquire iterators for closed historical shards that aren't
        // relevant to the configured starting position. Per the API spec,
        // ShardFilter cannot accompany NextToken on subsequent pages.
        req.streamName(config.getStreamName());
        if (filter != null) {
          req.shardFilter(filter);
        }
      } else {
        req.nextToken(nextToken);
      }
      ListShardsResponse resp = client.listShards(req.build());
      shards.addAll(resp.shards());
      nextToken = resp.nextToken();
    } while (nextToken != null);
    return shards;
  }

  /**
   * Translates the global {@link KinesisStartingPosition} into a stream-wide {@link ShardFilter}
   * that ListShards can use to drop irrelevant closed shards. Returns {@code null} when no
   * filter applies — either the default {@code FROM_TRIM_HORIZON} matches our intent, or the
   * starting position is per-shard and a stream-wide filter would over-prune.
   */
  private ShardFilter buildShardFilter() {
    // With per-shard overrides, the caller is targeting specific shards
    // (typically restoring from checkpoints) and may want closed shards
    // included. Skip the filter and let listAllShards return everything.
    if (!config.getStartingPositionByShard().isEmpty()) {
      return null;
    }
    KinesisStartingPosition pos = config.getStartingPosition();
    return switch (pos.getType()) {
      case LATEST -> ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case AT_TIMESTAMP ->
          ShardFilter.builder()
              .type(ShardFilterType.AT_TIMESTAMP)
              .timestamp(pos.getTimestamp())
              .build();
      // TRIM_HORIZON's default matches FROM_TRIM_HORIZON. Sequence-number
      // positions are per-shard, so no stream-wide filter is meaningful.
      default -> null;
    };
  }

  private String acquireShardIterator(String shardId, KinesisStartingPosition position) {
    GetShardIteratorRequest.Builder req =
        GetShardIteratorRequest.builder()
            .streamName(config.getStreamName())
            .shardId(shardId)
            .shardIteratorType(position.getType());
    if (position.getTimestamp() != null) {
      req.timestamp(position.getTimestamp());
    }
    if (position.getSequenceNumber() != null) {
      req.startingSequenceNumber(position.getSequenceNumber());
    }
    GetShardIteratorResponse resp = client.getShardIterator(req.build());
    return resp.shardIterator();
  }

  /**
   * Initial starting position for {@code shardId} on first subscribe — the per-shard override
   * if one was supplied (e.g. restored from a prior {@link #getCheckpoints()} snapshot),
   * otherwise the config's global default.
   */
  private KinesisStartingPosition startingPositionFor(String shardId) {
    KinesisStartingPosition perShard = config.getStartingPositionByShard().get(shardId);
    return perShard != null ? perShard : config.getStartingPosition();
  }

  /**
   * Position to resume from after an iterator expires. Prefers the highest acked sequence
   * (so already acked records are not redelivered); if no acks have advanced the watermark
   * yet but records have been delivered, falls back to the lowest delivered-but-unacked
   * sequence so those records are redelivered (preserving at-least-once); otherwise falls
   * back to the initial starting position.
   */
  private KinesisStartingPosition resumePositionFor(String shardId) {
    BigInteger watermark = highWatermarkByShard.get(shardId);
    if (watermark != null) {
      return KinesisStartingPosition.afterSequenceNumber(watermark.toString());
    }
    NavigableSet<BigInteger> delivered = deliveredSeqsByShard.get(shardId);
    if (delivered != null && !delivered.isEmpty()) {
      // Records have been delivered but no ack has advanced the watermark.
      // Resuming at the configured starting position (especially LATEST)
      // would skip these unacked records permanently; AT_SEQUENCE_NUMBER on
      // the lowest one redelivers everything that's still outstanding.
      return KinesisStartingPosition.atSequenceNumber(delivered.first().toString());
    }
    return startingPositionFor(shardId);
  }

  /**
   * Sleeps until {@code shardId}'s next {@code GetRecords} call is allowed under the per-shard
   * TPS + byte budget recorded by {@link #recordFetch(String, long)}, capped at the remaining
   * poll budget. Returns {@code true} if it's OK to call {@code GetRecords} now; {@code false}
   * if the poll deadline would be exceeded or the thread was interrupted while waiting.
   */
  private boolean awaitNextFetch(String shardId, long deadlineNanos) {
    if (getRecordsMinIntervalNanos <= 0) {
      return true;
    }
    Long earliest = nextAllowedFetchNanosByShard.get(shardId);
    if (earliest == null) {
      return true;
    }
    long now = System.nanoTime();
    long waitNanos = earliest - now;
    if (waitNanos <= 0) {
      return true;
    }
    long remaining = deadlineNanos - now;
    if (remaining <= 0) {
      return false;
    }
    try {
      TimeUnit.NANOSECONDS.sleep(Math.min(waitNanos, remaining));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    // If we capped the sleep at the remaining budget, we still haven't waited
    // out the per-shard interval — bail rather than risk a throttle exception.
    return waitNanos <= remaining;
  }

  /**
   * Records a {@code GetRecords} call against {@code shardId}'s budget. The next call is
   * allowed only after {@code max(getRecordsMinInterval, fetchedBytes / 2 MiB-per-sec)} —
   * Kinesis enforces both per-shard limits and exceeding either throws
   * {@code ProvisionedThroughputExceededException}.
   */
  private void recordFetch(String shardId, long fetchedBytes) {
    if (getRecordsMinIntervalNanos <= 0) {
      return;
    }
    long now = System.nanoTime();
    long tpsLimit = now + getRecordsMinIntervalNanos;
    long bytesLimit = now + bytesToNanosAtReadRate(fetchedBytes);
    nextAllowedFetchNanosByShard.put(shardId, Math.max(tpsLimit, bytesLimit));
  }

  private static long bytesToNanosAtReadRate(long bytes) {
    if (bytes <= 0) {
      return 0L;
    }
    // Round up so we never underestimate the wait.
    return (bytes * 1_000_000_000L + READ_BYTES_PER_SECOND_PER_SHARD - 1)
        / READ_BYTES_PER_SECOND_PER_SHARD;
  }

  private void ensureConnected() {
    if (client == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }

  private record ShardSequence(String shardId, String sequenceNumber) {}
}
