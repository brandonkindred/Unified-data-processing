package com.unifieddataprocessing.pubsub.relay;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fans the unified Kafka backbone out to heterogeneous downstream brokers — the symmetric inverse
 * of {@code DataBridge}.
 *
 * <p>Callers register each backbone-to-destination binding under a
 * {@code (destinationId, sourceTopic, downstreamTopic)} triple, supplying a freshly-constructed
 * {@link PubSubConsumer} (typically reading from the unified Kafka backbone) and a freshly-
 * constructed {@link PubSubPublisher} (writing to the destination broker — Kafka, MSK, Pulsar, GCP
 * Pub/Sub, Kinesis, …). The relay owns the {@code connect/subscribe/close} lifecycle of every
 * registered consumer and publisher, polls each from the relay's executor, republishes every
 * message to the registration's downstream topic, and stamps relay provenance attributes. Delivery
 * is at-least-once: a backbone message is acked only after the corresponding downstream publish
 * completes within {@code publishTimeout}.
 *
 * <p>Each registration runs on its own worker thread, drawn from a fixed-size pool sized to the
 * number of registrations. Per-registration publishers and consumers are isolated: a failing
 * downstream destination pauses only its own poll loop (retrying the in-flight message with
 * {@code pollBackoff} until it succeeds or {@code close()} is called) and does not block other
 * registrations.
 */
public final class DataRelay implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(DataRelay.class.getName());

  private enum State {
    Configured,
    Starting,
    Running,
    Closed
  }

  private final DataRelayConfig config;
  private final Function<Integer, ExecutorService> executorFactory;
  private final Sleeper sleeper;

  private final List<RelayRegistration> registrations = new ArrayList<>();
  private final Set<PubSubConsumer> consumerIdentities =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<PubSubPublisher> publisherIdentities =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<String> registeredPairs = new HashSet<>();

  private volatile State state = State.Configured;
  private ExecutorService executor;

  /**
   * Production constructor. The default executor is {@link Executors#newFixedThreadPool(int)} sized
   * to the number of registrations, so each registration polls on its own worker thread.
   */
  public DataRelay(DataRelayConfig config) {
    this(config, n -> Executors.newFixedThreadPool(n), Sleeper.real());
  }

  /** Test constructor: every external dependency is injected. */
  DataRelay(
      DataRelayConfig config,
      Function<Integer, ExecutorService> executorFactory,
      Sleeper sleeper) {
    this.config = Objects.requireNonNull(config, "config");
    this.executorFactory = Objects.requireNonNull(executorFactory, "executorFactory");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
  }

  /**
   * Registers a backbone-to-destination binding. All validation runs before any field is mutated,
   * so a failed register leaves the relay in its prior state.
   *
   * <p>Each {@code (destinationId, sourceTopic)} pair must be unique within the relay, and each
   * {@link PubSubConsumer} and {@link PubSubPublisher} instance may be registered at most once
   * (identity check) — the relay owns their entire lifecycle.
   */
  public synchronized void register(
      String destinationId,
      String sourceTopic,
      String downstreamTopic,
      PubSubConsumer consumer,
      PubSubPublisher publisher) {
    if (state != State.Configured) {
      throw new IllegalStateException("register() must be called before start()");
    }
    String pairKey = destinationId + " " + sourceTopic;
    final RelayRegistration reg =
        new RelayRegistration(destinationId, sourceTopic, downstreamTopic, consumer, publisher);
    if (registeredPairs.contains(pairKey)) {
      throw new IllegalArgumentException(
          "(destinationId, sourceTopic) already registered: "
              + destinationId
              + " / "
              + sourceTopic);
    }
    if (consumerIdentities.contains(consumer)) {
      throw new IllegalArgumentException("consumer instance already registered");
    }
    if (publisherIdentities.contains(publisher)) {
      throw new IllegalArgumentException("publisher instance already registered");
    }
    registrations.add(reg);
    registeredPairs.add(pairKey);
    consumerIdentities.add(consumer);
    publisherIdentities.add(publisher);
  }

  /**
   * Connects every registered publisher, then every registered consumer (subscribing each to its
   * {@code sourceTopic}), then launches one poll loop per registration on the injected executor.
   * On any failure the relay cleans up in reverse order (executor → consumers → publishers) and
   * ends in {@code Closed}, so a subsequent {@code close()} is a no-op.
   */
  public synchronized void start() {
    if (state != State.Configured) {
      throw new IllegalStateException("start() can only be called once, from Configured");
    }
    if (registrations.isEmpty()) {
      throw new IllegalStateException("at least one registration required before start()");
    }
    state = State.Starting;

    List<PubSubPublisher> connectedPublishers = new ArrayList<>();
    List<PubSubConsumer> connectedConsumers = new ArrayList<>();
    ExecutorService allocatedExecutor = null;
    try {
      for (RelayRegistration reg : registrations) {
        reg.publisher().connect();
        connectedPublishers.add(reg.publisher());
      }
      for (RelayRegistration reg : registrations) {
        reg.consumer().connect();
        connectedConsumers.add(reg.consumer());
        reg.consumer().subscribe(reg.sourceTopic());
      }

      allocatedExecutor = executorFactory.apply(registrations.size());
      executor = allocatedExecutor;

      state = State.Running;

      for (RelayRegistration reg : registrations) {
        executor.submit(
            () -> {
              Thread.currentThread()
                  .setName("data-relay-" + reg.destinationId() + "-" + reg.sourceTopic());
              pollLoopForever(reg);
            });
      }
    } catch (RuntimeException | Error e) {
      cleanupAfterFailedStart(allocatedExecutor, connectedConsumers, connectedPublishers);
      throw e;
    }
  }

  public boolean isRunning() {
    return state == State.Running;
  }

  @Override
  public synchronized void close() {
    if (state == State.Closed) {
      return;
    }
    state = State.Closed;

    if (executor != null) {
      executor.shutdown();
      boolean terminated = awaitQuietly(executor, config.shutdownTimeout().toMillis());
      if (!terminated) {
        executor.shutdownNow();
        awaitQuietly(executor, config.closeForceTimeout().toMillis());
      }
    }

    flushPublishersBounded();
    for (RelayRegistration reg : registrations) {
      try {
        reg.consumer().close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    for (RelayRegistration reg : registrations) {
      try {
        reg.publisher().close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
  }

  private void cleanupAfterFailedStart(
      ExecutorService allocatedExecutor,
      List<PubSubConsumer> connectedConsumers,
      List<PubSubPublisher> connectedPublishers) {
    // Flip state to Closed BEFORE we start tearing things down. If any worker tasks have already
    // been submitted (e.g. an executor rejected a later submit while earlier ones were accepted),
    // they re-check {@code state == Running} between every poll/publish/ack step — flipping state
    // first lets them exit cleanly without touching clients we're about to close. Then
    // {@code shutdownNow} interrupts anyone still blocked, and we wait up to
    // {@code closeForceTimeout} for them to actually exit before closing the consumers/publishers
    // they may have been calling into.
    state = State.Closed;
    if (allocatedExecutor != null) {
      allocatedExecutor.shutdownNow();
      awaitQuietly(allocatedExecutor, config.closeForceTimeout().toMillis());
    }
    for (PubSubConsumer c : connectedConsumers) {
      try {
        c.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    for (PubSubPublisher p : connectedPublishers) {
      try {
        p.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
  }

  /**
   * Flushes every registered publisher with a hard total wall-time budget of
   * {@code closeForceTimeout}, so a publisher whose {@code flush()} would block forever on a
   * stale in-flight publish future (the {@code CompletableFuture} a timed-out {@code .get()} left
   * outstanding in {@link #publishWithRetry}) cannot bypass the relay's declared shutdown budget.
   *
   * <p>Each flush gets a fair-share slice of {@code closeForceTimeout / registrations.size()},
   * but every per-flush join is also capped against the time remaining until the
   * {@code closeForceTimeout} deadline, so the total wall time spent in this method is bounded by
   * {@code closeForceTimeout} regardless of how many registrations there are or how many flushes
   * hang. Each flush runs on its own short-lived daemon thread, so a flush that hangs and ignores
   * interruption on one publisher cannot starve later publishers of their fair share within the
   * remaining budget. If a slice expires the thread is interrupted (best-effort) and abandoned —
   * abandoned threads are daemons, so they don't keep the JVM alive past {@code close()}.
   */
  private void flushPublishersBounded() {
    if (registrations.isEmpty()) {
      return;
    }
    long totalBudgetNs = config.closeForceTimeout().toNanos();
    long deadlineNs = System.nanoTime() + totalBudgetNs;
    long fairShareNs = totalBudgetNs / registrations.size();
    for (RelayRegistration reg : registrations) {
      long remainingNs = deadlineNs - System.nanoTime();
      if (remainingNs <= 0) {
        LOG.log(
            Level.WARNING,
            "flush budget exhausted; skipping remaining publisher flushes during close()");
        break;
      }
      long sliceNs = Math.min(fairShareNs, remainingNs);
      Thread flushThread =
          new Thread(
              () -> {
                try {
                  reg.publisher().flush();
                } catch (RuntimeException ignored) {
                  // best-effort
                }
              },
              "data-relay-flush-" + reg.destinationId());
      flushThread.setDaemon(true);
      flushThread.start();
      try {
        long sliceMs = TimeUnit.NANOSECONDS.toMillis(sliceNs);
        int sliceNanos = (int) (sliceNs % 1_000_000L);
        // Thread.join(0, 0) waits forever — guarantee we always pass at least 1 ns.
        if (sliceMs == 0 && sliceNanos == 0) {
          sliceNanos = 1;
        }
        flushThread.join(sliceMs, sliceNanos);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        flushThread.interrupt();
        break;
      }
      if (flushThread.isAlive()) {
        flushThread.interrupt();
        LOG.log(
            Level.WARNING,
            "flush timed out for "
                + reg.destinationId()
                + "/"
                + reg.downstreamTopic()
                + "; abandoning and continuing close()");
      }
    }
  }

  private void pollLoopForever(RelayRegistration reg) {
    while (state == State.Running) {
      List<Message> batch;
      try {
        batch = reg.consumer().poll(config.pollTimeout());
      } catch (RuntimeException e) {
        LOG.log(
            Level.WARNING,
            "poll failed for " + reg.destinationId() + "/" + reg.sourceTopic() + "; backing off",
            e);
        try {
          sleeper.sleep(config.pollBackoff());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
        continue;
      }
      if (!processBatch(reg, batch)) {
        return;
      }
    }
  }

  private boolean processBatch(RelayRegistration reg, List<Message> batch) {
    boolean ackFailureSeen = false;
    for (Message m : batch) {
      if (state != State.Running) {
        return true;
      }
      Message rewritten = RelayMessageRewriter.rewrite(m, reg);
      if (!publishWithRetry(reg, rewritten)) {
        return false;
      }
      if (state != State.Running) {
        return true;
      }
      AckOutcome outcome = tryAcknowledgeOnce(reg, m);
      if (outcome == AckOutcome.EXIT) {
        return false;
      }
      if (outcome == AckOutcome.FAILURE) {
        // Continue processing the rest of the batch — every entry is already in our hands and
        // any consumer wrapper that tracks delivered offsets (e.g. the project's KafkaConsumer)
        // has already marked them delivered, so dropping them here would block the commit
        // watermark until restart. The outer poll loop will run poll(...) after this batch,
        // which is what lets e.g. a Kafka rebalance complete. We sleep pollBackoff exactly
        // once per batch so a burst of transient ack failures (e.g. during a rebalance) doesn't
        // amplify backoff by batch size.
        if (!ackFailureSeen) {
          ackFailureSeen = true;
          try {
            sleeper.sleep(config.pollBackoff());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Publishes a single message, retrying on timeout/failure with {@code pollBackoff} until success
   * or {@code close()}. Returning {@code false} indicates the worker was interrupted and the poll
   * loop should exit.
   *
   * <p>Retrying the same message in-place (rather than breaking the batch and letting the poll
   * loop continue) keeps the source consumer's delivered/acked cursor contiguous: a
   * {@link com.unifieddataprocessing.pubsub.PubSubConsumer} that tracks delivered offsets
   * internally (e.g. the project's Kafka consumer wrapper) would otherwise advance past the failed
   * message on the next {@code poll()}, stranding it until rebalance/restart and turning every
   * later record into a duplicate after recovery.
   *
   * <p>Catches {@link RuntimeException} alongside {@link ExecutionException} /
   * {@link TimeoutException} so that publishers that throw synchronously from {@code publish(...)}
   * (instead of returning an exceptionally-completed future) or that hand back a cancelled future
   * (whose {@code get()} throws {@link java.util.concurrent.CancellationException}) don't bypass
   * the retry path and silently kill the worker.
   */
  private boolean publishWithRetry(RelayRegistration reg, Message rewritten) {
    while (state == State.Running) {
      try {
        reg.publisher()
            .publish(rewritten)
            .get(config.publishTimeout().toNanos(), TimeUnit.NANOSECONDS);
        return true;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException | TimeoutException | RuntimeException e) {
        LOG.log(
            Level.WARNING,
            "publish failed for "
                + reg.destinationId()
                + "/"
                + reg.downstreamTopic()
                + "; pausing and retrying the same message",
            e);
        try {
          sleeper.sleep(config.pollBackoff());
        } catch (InterruptedException sleepInterrupt) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return true;
  }

  /** Outcome of a single ack attempt; see {@link #tryAcknowledgeOnce}. */
  private enum AckOutcome {
    SUCCESS,
    FAILURE,
    EXIT
  }

  /**
   * Acknowledges a successfully-published message on the source consumer with a single attempt.
   * Returns {@link AckOutcome#SUCCESS} on success, {@link AckOutcome#EXIT} if the worker was
   * interrupted, or {@link AckOutcome#FAILURE} on any other ack failure (after logging).
   *
   * <p>Why a single attempt + continue-batch instead of retry-in-place: ack failures from
   * {@code KafkaConsumer.commitSync()} during a rebalance cannot be cleared by retrying the same
   * commit in a tight loop — the consumer must call {@code poll(...)} again to drive the
   * rebalance to completion before any commit can succeed. {@link #processBatch} therefore keeps
   * processing the rest of the polled batch (every entry is already in our hands) and lets the
   * outer poll loop call {@code poll(...)} after the batch ends; that next poll is what lets the
   * consumer make state-machine progress. Messages already published but not yet acked may be
   * redelivered and re-published as duplicates (acceptable under at-least-once).
   */
  private AckOutcome tryAcknowledgeOnce(RelayRegistration reg, Message m) {
    try {
      reg.consumer().acknowledge(m);
      return AckOutcome.SUCCESS;
    } catch (RuntimeException e) {
      LOG.log(
          Level.WARNING,
          "acknowledge failed for "
              + reg.destinationId()
              + "/"
              + reg.sourceTopic()
              + "; continuing batch, next poll() will let the consumer recover"
              + " (e.g. complete a Kafka rebalance)",
          e);
      return AckOutcome.FAILURE;
    }
  }

  private static boolean awaitQuietly(ExecutorService exec, long millis) {
    try {
      return exec.awaitTermination(millis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
