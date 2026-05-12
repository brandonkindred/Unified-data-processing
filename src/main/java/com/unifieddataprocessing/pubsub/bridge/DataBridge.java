package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducer;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * Fans heterogeneous {@link PubSubConsumer} sources into a single Kafka backbone.
 *
 * <p>Callers register existing source consumers under a {@code (sourceId, channel, sourceTopic)}
 * triple; the bridge owns each registered consumer's {@code connect/subscribe/close} lifecycle,
 * polls each from the bridge's executor, republishes every message to a Kafka topic named
 * {@code sourceId + "." + channel}, and auto-provisions the topic on first run. Delivery is
 * at-least-once: a source message is acked only after the corresponding Kafka publish completes
 * within {@code publishTimeout}.
 *
 * <p>Each registration runs on its own worker thread, drawn from a fixed-size pool sized to the
 * number of registrations. The {@link PubSubPublisher} wrapper's lifecycle ({@code connect},
 * {@code flush}, {@code close}) is single-threaded on the caller's thread, bounded by
 * {@code shutdownTimeout + closeForceTimeout}; {@code publish(...)} is invoked concurrently from
 * N worker threads and relies on the underlying Kafka client being thread-safe for {@code send}.
 * A failing {@code poll(...)} backs off via the injected {@link Sleeper} and retries; a failing
 * {@code publish(...)} (timeout or execution failure) breaks the current batch without acking, so
 * the source will redeliver. Per-channel topic overrides are layered on by a future chunk.
 */
public final class DataBridge implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(DataBridge.class.getName());

  private enum State {
    Configured,
    Provisioning,
    Running,
    Closed
  }

  private final DataBridgeConfig config;
  private final Function<KafkaProducerConfig, PubSubPublisher> publisherFactory;
  private final BridgeTopicProvisioner provisioner;
  private final Function<Integer, ExecutorService> executorFactory;
  private final Sleeper sleeper;

  private final List<Registration> registrations = new ArrayList<>();
  private final Set<PubSubConsumer> consumerIdentities =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<String> registeredPairs = new HashSet<>();

  private volatile State state = State.Configured;
  private PubSubPublisher publisher;
  private ExecutorService executor;

  /**
   * Production constructor. The default executor is {@link Executors#newFixedThreadPool(int)}
   * sized to the number of registrations, so each source polls on its own worker thread.
   */
  public DataBridge(DataBridgeConfig config) {
    this(
        config,
        KafkaProducer::new,
        AdminClient::create,
        n -> Executors.newFixedThreadPool(n),
        Sleeper.real());
  }

  /** Test constructor: every external dependency is injected. */
  DataBridge(
      DataBridgeConfig config,
      Function<KafkaProducerConfig, PubSubPublisher> publisherFactory,
      Function<Properties, AdminClient> adminFactory,
      Function<Integer, ExecutorService> executorFactory,
      Sleeper sleeper) {
    this.config = Objects.requireNonNull(config, "config");
    this.publisherFactory = Objects.requireNonNull(publisherFactory, "publisherFactory");
    Objects.requireNonNull(adminFactory, "adminFactory");
    this.provisioner = new BridgeTopicProvisioner(adminFactory);
    this.executorFactory = Objects.requireNonNull(executorFactory, "executorFactory");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
  }

  /**
   * Registers a source-to-channel binding. All validation runs before any field is mutated, so a
   * failed register leaves the bridge in its prior state.
   */
  public synchronized void register(
      String sourceId,
      String channel,
      String sourceTopic,
      PubSubConsumer consumer,
      ChannelOptions options) {
    if (state != State.Configured) {
      throw new IllegalStateException("register() must be called before start()");
    }
    String targetTopic = sourceId + "." + channel;
    Registration reg =
        new Registration(sourceId, channel, sourceTopic, targetTopic, consumer, options);
    if (registeredPairs.contains(targetTopic)) {
      throw new IllegalArgumentException(
          "(sourceId, channel) already registered: " + targetTopic);
    }
    if (consumerIdentities.contains(consumer)) {
      throw new IllegalArgumentException("consumer instance already registered");
    }
    registrations.add(reg);
    registeredPairs.add(targetTopic);
    consumerIdentities.add(consumer);
  }

  /**
   * Provisions topics, connects the publisher and every consumer, then launches one poll loop per
   * registration on the injected executor. On any failure the bridge cleans up in reverse order
   * (executor → consumers → publisher) and ends in {@code Closed}, so a subsequent {@code close()}
   * is a no-op.
   */
  public synchronized void start() {
    if (state != State.Configured) {
      throw new IllegalStateException("start() can only be called once, from Configured");
    }
    if (registrations.isEmpty()) {
      throw new IllegalStateException("at least one registration required before start()");
    }
    state = State.Provisioning;

    boolean publisherConnected = false;
    List<PubSubConsumer> connectedSoFar = new ArrayList<>();
    ExecutorService allocatedExecutor = null;
    try {
      List<NewTopicSpec> specs = new ArrayList<>(registrations.size());
      for (Registration reg : registrations) {
        specs.add(
            new NewTopicSpec(
                reg.targetTopic(),
                config.defaultPartitions(),
                config.defaultReplicationFactor(),
                Collections.emptyMap()));
      }
      provisioner.provision(config.producerConfig().toProperties(), specs);

      publisher = publisherFactory.apply(config.producerConfig());
      publisher.connect();
      publisherConnected = true;

      for (Registration reg : registrations) {
        reg.consumer().connect();
        connectedSoFar.add(reg.consumer());
        reg.consumer().subscribe(reg.sourceTopic());
      }

      allocatedExecutor = executorFactory.apply(registrations.size());
      executor = allocatedExecutor;

      state = State.Running;

      PubSubPublisher pub = publisher;
      for (Registration reg : registrations) {
        executor.submit(
            () -> {
              Thread.currentThread()
                  .setName("data-bridge-" + reg.sourceId() + "-" + reg.channel());
              pollLoopForever(reg, pub);
            });
      }
    } catch (RuntimeException | Error e) {
      cleanupAfterFailedStart(allocatedExecutor, connectedSoFar, publisherConnected);
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

    if (publisher != null) {
      try {
        publisher.flush();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    for (Registration reg : registrations) {
      try {
        reg.consumer().close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    if (publisher != null) {
      try {
        publisher.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
  }

  private void cleanupAfterFailedStart(
      ExecutorService allocatedExecutor,
      List<PubSubConsumer> connectedSoFar,
      boolean publisherConnected) {
    if (allocatedExecutor != null) {
      allocatedExecutor.shutdownNow();
    }
    for (PubSubConsumer c : connectedSoFar) {
      try {
        c.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    if (publisherConnected) {
      try {
        publisher.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    state = State.Closed;
  }

  private void pollLoopForever(Registration reg, PubSubPublisher pub) {
    while (state == State.Running) {
      List<Message> batch;
      try {
        batch = reg.consumer().poll(config.pollTimeout());
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "poll failed for " + reg.targetTopic() + "; backing off", e);
        try {
          sleeper.sleep(config.pollBackoff());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
        continue;
      }
      if (!processBatch(reg, pub, batch)) {
        return;
      }
    }
  }

  private boolean processBatch(Registration reg, PubSubPublisher pub, List<Message> batch) {
    for (Message m : batch) {
      if (state != State.Running) {
        return true;
      }
      Message rewritten = MessageRewriter.rewrite(m, reg);
      try {
        pub.publish(rewritten).get(config.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException | TimeoutException e) {
        LOG.log(
            Level.WARNING,
            "publish failed for " + reg.targetTopic() + "; breaking batch (no ack)",
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
