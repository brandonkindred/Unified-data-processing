package com.unifieddataprocessing.pubsub.kinesis;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

/**
 * Amazon Kinesis Data Streams-backed {@link PubSubPublisher}. Wraps the synchronous {@link
 * KinesisClient} (the same client {@link
 * com.unifieddataprocessing.pubsub.kinesis.KinesisConsumer} uses, to keep the dependency footprint
 * identical) and offloads each blocking call to an {@link ExecutorService} so the wrapper can
 * expose a {@link CompletableFuture} surface.
 *
 * <p>Stream name comes from {@link Message#getTopic()} on every publish — this publisher is not
 * stream-bound, unlike the consumer. Partition key is read from the reserved attribute {@link
 * KinesisConsumer#ATTR_PARTITION_KEY}; if absent, falls back to {@link Message#getId()} so callers
 * who never set the attribute still get valid, deterministic partitioning.
 *
 * <p>{@link #publishBatch(List)} groups by stream and uses Kinesis's native {@code PutRecords},
 * chunking at {@link KinesisPublisherConfig#getMaxRecordsPerBatch()} (capped at the Kinesis
 * service limit of 500 records per call). Per-record partial failures are surfaced as
 * exceptionally-completed per-message futures while siblings succeed.
 *
 * <p>Not thread-safe; the underlying SDK client is reusable across threads but the per-instance
 * inflight bookkeeping in this wrapper is not.
 */
public class KinesisPublisher implements PubSubPublisher {

  private final KinesisPublisherConfig config;
  private final Function<KinesisPublisherConfig, KinesisClient> clientFactory;
  private final Function<KinesisPublisherConfig, ExecutorService> executorFactory;
  private final Set<CompletableFuture<PublishResult>> inflight = new LinkedHashSet<>();

  private KinesisClient client;
  private ExecutorService executor;

  /** Creates a publisher that builds a real {@link KinesisClient} on {@link #connect()}. */
  public KinesisPublisher(KinesisPublisherConfig config) {
    this(
        config,
        c ->
            KinesisClient.builder()
                .region(c.getRegion())
                .credentialsProvider(c.getCredentialsProvider())
                .build(),
        c -> Executors.newFixedThreadPool(c.getPublishConcurrency()));
  }

  KinesisPublisher(
      KinesisPublisherConfig config,
      Function<KinesisPublisherConfig, KinesisClient> clientFactory,
      Function<KinesisPublisherConfig, ExecutorService> executorFactory) {
    this.config = Objects.requireNonNull(config, "config");
    this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
    this.executorFactory = Objects.requireNonNull(executorFactory, "executorFactory");
  }

  @Override
  public void connect() {
    if (client != null) {
      throw new IllegalStateException("already connected");
    }
    KinesisClient newClient = clientFactory.apply(config);
    ExecutorService newExecutor;
    try {
      newExecutor = executorFactory.apply(config);
    } catch (RuntimeException e) {
      try {
        newClient.close();
      } catch (RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    client = newClient;
    executor = newExecutor;
  }

  @Override
  public CompletableFuture<PublishResult> publish(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    return doPublish(message);
  }

  @Override
  public PublishResult publishSync(Message message) {
    try {
      return publish(message).join();
    } catch (CompletionException e) {
      throw unwrap(e);
    }
  }

  @Override
  public CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages) {
    Objects.requireNonNull(messages, "messages");
    ensureConnected();
    if (messages.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    // Group inputs by stream while preserving the original index so we can reassemble per-message
    // futures in caller order. Each chunk goes out as a single PutRecords call (Kinesis caps at
    // 500 records per call). Note: this loop also validates every entry up front (a null partway
    // through throws here before any submission), so a caller retrying the failed batch cannot
    // duplicate a prefix of records that were already sent.
    Map<String, List<Integer>> indicesByStream = new LinkedHashMap<>();
    for (int i = 0; i < messages.size(); i++) {
      Message m = messages.get(i);
      Objects.requireNonNull(m, "messages contains null");
      indicesByStream.computeIfAbsent(m.getTopic(), k -> new ArrayList<>()).add(i);
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<PublishResult>[] perMessage = new CompletableFuture[messages.size()];
    int chunkSize = config.getMaxRecordsPerBatch();

    for (Map.Entry<String, List<Integer>> entry : indicesByStream.entrySet()) {
      String stream = entry.getKey();
      List<Integer> indices = entry.getValue();
      for (int start = 0; start < indices.size(); start += chunkSize) {
        int end = Math.min(start + chunkSize, indices.size());
        List<Integer> chunkIndices = indices.subList(start, end);
        List<Message> chunkMessages = new ArrayList<>(chunkIndices.size());
        for (int idx : chunkIndices) {
          chunkMessages.add(messages.get(idx));
        }
        List<CompletableFuture<PublishResult>> chunkFutures =
            submitBatchChunk(stream, chunkMessages);
        for (int i = 0; i < chunkIndices.size(); i++) {
          perMessage[chunkIndices.get(i)] = chunkFutures.get(i);
        }
      }
    }

    return aggregate(java.util.Arrays.asList(perMessage));
  }

  @Override
  public void flush() {
    ensureConnected();
    List<CompletableFuture<PublishResult>> snapshot = new ArrayList<>(inflight);
    if (snapshot.isEmpty()) {
      return;
    }
    try {
      CompletableFuture.allOf(snapshot.toArray(new CompletableFuture<?>[0])).join();
    } catch (CompletionException ignored) {
      // Per-message failures already surface through their own futures.
    }
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      if (executor != null) {
        executor.shutdown();
        try {
          executor.awaitTermination(config.getCloseTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      try {
        client.close();
      } finally {
        client = null;
        executor = null;
        inflight.clear();
      }
    }
  }

  private CompletableFuture<PublishResult> doPublish(Message message) {
    CompletableFuture<PublishResult> cf = new CompletableFuture<>();
    inflight.add(cf);
    cf.whenComplete((r, t) -> inflight.remove(cf));
    // Funnel any synchronous failure (request-build error, RejectedExecutionException from a
    // saturated executor, etc.) into the future so publishBatch's aggregate contract holds —
    // siblings keep submitting and failures are reported via PublishBatchException.
    try {
      String stream = message.getTopic();
      String partitionKey = resolvePartitionKey(message);
      PutRecordRequest request =
          PutRecordRequest.builder()
              .streamName(stream)
              .partitionKey(partitionKey)
              .data(SdkBytes.fromByteArray(message.getPayload()))
              .build();
      executor.execute(
          () -> {
            try {
              PutRecordResponse response = client.putRecord(request);
              String id = response.shardId() + ":" + response.sequenceNumber();
              cf.complete(
                  PublishResult.forKinesis(
                      stream, id, response.shardId(), response.sequenceNumber()));
            } catch (RuntimeException e) {
              cf.completeExceptionally(e);
            }
          });
    } catch (RuntimeException e) {
      cf.completeExceptionally(e);
    }
    return cf;
  }

  private List<CompletableFuture<PublishResult>> submitBatchChunk(
      String stream, List<Message> chunk) {
    int n = chunk.size();
    List<CompletableFuture<PublishResult>> futures = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      CompletableFuture<PublishResult> cf = new CompletableFuture<>();
      futures.add(cf);
      inflight.add(cf);
      cf.whenComplete((r, t) -> inflight.remove(cf));
    }

    PutRecordsRequest request;
    try {
      List<PutRecordsRequestEntry> entries = new ArrayList<>(n);
      for (Message m : chunk) {
        entries.add(
            PutRecordsRequestEntry.builder()
                .partitionKey(resolvePartitionKey(m))
                .data(SdkBytes.fromByteArray(m.getPayload()))
                .build());
      }
      request = PutRecordsRequest.builder().streamName(stream).records(entries).build();
    } catch (RuntimeException e) {
      // Same aggregation contract as doPublish: a sync request-build failure for this chunk
      // becomes per-message failed futures so publishBatch can report it via PublishBatchException.
      for (CompletableFuture<PublishResult> cf : futures) {
        cf.completeExceptionally(e);
      }
      return futures;
    }

    try {
      executor.execute(
          () -> {
            PutRecordsResponse response;
            try {
              response = client.putRecords(request);
            } catch (RuntimeException e) {
              for (CompletableFuture<PublishResult> cf : futures) {
                cf.completeExceptionally(e);
              }
              return;
            }
            List<PutRecordsResultEntry> results = response.records();
            for (int i = 0; i < futures.size(); i++) {
              CompletableFuture<PublishResult> cf = futures.get(i);
              PutRecordsResultEntry result = results.get(i);
              if (result.errorCode() != null) {
                cf.completeExceptionally(
                    new RuntimeException(
                        "Kinesis PutRecords entry failed: "
                            + result.errorCode()
                            + ": "
                            + result.errorMessage()));
              } else {
                String id = result.shardId() + ":" + result.sequenceNumber();
                cf.complete(
                    PublishResult.forKinesis(
                        stream, id, result.shardId(), result.sequenceNumber()));
              }
            }
          });
    } catch (RuntimeException e) {
      // Synchronous executor.execute failure (RejectedExecutionException after shutdown, etc.):
      // settle every future in this chunk so publishBatch reports the failure in aggregate.
      for (CompletableFuture<PublishResult> cf : futures) {
        cf.completeExceptionally(e);
      }
    }
    return futures;
  }

  private static String resolvePartitionKey(Message message) {
    String pk = message.getAttributes().get(KinesisConsumer.ATTR_PARTITION_KEY);
    if (pk != null && !pk.isEmpty()) {
      return pk;
    }
    return message.getId();
  }

  private static CompletableFuture<List<PublishResult>> aggregate(
      List<CompletableFuture<PublishResult>> perMessage) {
    CompletableFuture<Void> all =
        CompletableFuture.allOf(perMessage.toArray(new CompletableFuture<?>[0]));
    return all.handle(
        (v, t) -> {
          List<PublishResult> succeeded = new ArrayList<>();
          Map<Integer, Throwable> failures = new LinkedHashMap<>();
          for (int i = 0; i < perMessage.size(); i++) {
            CompletableFuture<PublishResult> f = perMessage.get(i);
            if (f.isCompletedExceptionally()) {
              try {
                f.join();
              } catch (CompletionException ce) {
                failures.put(i, ce.getCause() == null ? ce : ce.getCause());
              } catch (RuntimeException re) {
                failures.put(i, re);
              }
            } else {
              succeeded.add(f.join());
            }
          }
          if (failures.isEmpty()) {
            return succeeded;
          }
          throw new PublishBatchException(succeeded, failures);
        });
  }

  private static RuntimeException unwrap(CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException re) {
      return re;
    }
    if (cause == null) {
      return e;
    }
    return new RuntimeException(cause);
  }

  private void ensureConnected() {
    if (client == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }
}
