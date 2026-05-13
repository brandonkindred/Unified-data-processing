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
 * downstream destination breaks only its own batch (without acking, so the backbone redelivers)
 * and does not block other registrations.
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

    for (RelayRegistration reg : registrations) {
      try {
        reg.publisher().flush();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
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
    if (allocatedExecutor != null) {
      allocatedExecutor.shutdownNow();
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
    state = State.Closed;
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
    for (Message m : batch) {
      if (state != State.Running) {
        return true;
      }
      Message rewritten = RelayMessageRewriter.rewrite(m, reg);
      try {
        reg.publisher()
            .publish(rewritten)
            .get(config.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException | TimeoutException e) {
        LOG.log(
            Level.WARNING,
            "publish failed for "
                + reg.destinationId()
                + "/"
                + reg.downstreamTopic()
                + "; breaking batch (no ack)",
            e);
        return true;
      }
      reg.consumer().acknowledge(m);
    }
    return true;
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
