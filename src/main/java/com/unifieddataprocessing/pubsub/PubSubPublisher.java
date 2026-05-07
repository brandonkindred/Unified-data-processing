package com.unifieddataprocessing.pubsub;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over a pub/sub publisher (Kafka, GCP Pub/Sub, Kinesis, Pulsar). Counterpart to {@link
 * PubSubConsumer}. Topic comes from {@link Message#getTopic()} on every publish — one publisher
 * fans out to many topics.
 *
 * <p>Lifecycle: {@link #connect()} once before any other method, {@link #close()} once when done.
 * Implementations are documented as <strong>not thread-safe</strong>; the underlying SDK clients
 * are typically thread-safe, but the per-instance bookkeeping in each wrapper is not.
 *
 * <p>Per-message failure: returned {@link CompletableFuture}s complete exceptionally with the
 * SDK's native runtime exception. {@link #publishSync(Message)} unwraps {@link
 * java.util.concurrent.CompletionException}, rethrowing the cause as-is when it is a {@link
 * RuntimeException}.
 *
 * <p>Batch failure: {@link #publishBatch(List)} is <strong>aggregate</strong>, not fail-fast. The
 * returned future completes only after every per-message future settles. On any failure it
 * completes exceptionally with {@link PublishBatchException}.
 */
public interface PubSubPublisher extends AutoCloseable {

  /**
   * Idempotently establishes any client connection / pre-warms shared resources. Calling twice
   * throws {@link IllegalStateException}.
   */
  void connect();

  /**
   * Asynchronously publishes a single message; topic is taken from {@link Message#getTopic()}. The
   * returned future completes with the broker's acknowledgment, or completes exceptionally with the
   * SDK's native runtime exception.
   */
  CompletableFuture<PublishResult> publish(Message message);

  /**
   * Convenience wrapper for {@link #publish(Message)} that blocks until the broker has
   * acknowledged. {@link java.util.concurrent.CompletionException} is unwrapped: if the cause is a
   * {@link RuntimeException} it is rethrown as-is, otherwise wrapped in a {@link RuntimeException}.
   */
  PublishResult publishSync(Message message);

  /**
   * Asynchronously publishes a batch; messages may target different topics. The returned future
   * completes only after every per-message future settles. On success, it completes with results
   * in the same order as the input list. On any per-message failure, it completes exceptionally
   * with {@link PublishBatchException}.
   *
   * <p>An empty input list returns an immediately-completed future of an empty list.
   */
  CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages);

  /**
   * Blocks the calling thread until every previously enqueued publish has completed (success or
   * failure). Per-message failures still surface only through their own futures.
   */
  void flush();

  @Override
  void close();
}
