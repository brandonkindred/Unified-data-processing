package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducer;
import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaRegistry;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.SchemaViolationPolicy;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
 * the source will redeliver. To bound in-memory bookkeeping growth on cursor-based sources (e.g.
 * {@code KafkaConsumer}, {@code KinesisConsumer}) during a sustained downstream outage, each
 * worker tracks consecutive publish failures per registration: once
 * {@link DataBridgeConfig#publishFailureThreshold()} is reached the worker sleeps for
 * {@link DataBridgeConfig#publishFailureCooldown()} before its next poll. After cooldown the
 * worker enters a probe state — the very next publish failure trips the breaker again — so a
 * sustained outage adds at most one polled batch per cooldown cycle instead of {@code threshold}.
 * A single successful publish drops the counter back to zero (healthy). Per-channel
 * {@link ChannelOptions} layered over the {@link DataBridgeConfig} defaults control each topic's
 * partitions, replication factor, and Kafka topic-configs at provision time.
 *
 * <p>When a {@link SchemaRegistry} and {@link SchemaValidator} are configured on the {@link
 * DataBridgeConfig}, every batch resolves the latest schema for the registration's target topic
 * once and validates each payload before publishing. Topics with no registered schema are passed
 * through unchanged — schema enforcement is opt-in per topic. On violation, the configured {@link
 * SchemaViolationPolicy} decides whether the message is dropped (acked + skipped) or treated as a
 * publish failure (no ack, batch broken, circuit-breaker counts the failure). Successful
 * validation stamps {@link BridgeAttributes#BRIDGE_SCHEMA_SUBJECT} and {@link
 * BridgeAttributes#BRIDGE_SCHEMA_VERSION} on the republished message so downstream consumers can
 * route or decode by version.
 */
public final class DataBridge implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(DataBridge.class.getName());

  private enum State {
    Configured,
    Provisioning,
    Running,
    Closed
  }

  /**
   * Outcome of a single {@code processBatch(...)} call as observed by {@link #pollLoopForever}.
   * {@code interrupted} short-circuits the loop. Otherwise the circuit-breaker uses the two
   * booleans to decide its next state: a publish success anywhere in the batch resets the
   * consecutive-failure counter, and a publish failure increments it. An empty batch (no
   * messages polled) reports both flags as {@code false} so intermittent traffic during a
   * sustained outage cannot reset the counter without an actual successful publish.
   */
  private record BatchResult(
      boolean anyPublishSucceeded, boolean publishFailed, boolean interrupted) {}

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
        ChannelOptions options = reg.options();
        int partitions =
            options.getPartitions() == 0 ? config.defaultPartitions() : options.getPartitions();
        short replicationFactor =
            options.getReplicationFactor() == 0
                ? config.defaultReplicationFactor()
                : options.getReplicationFactor();
        specs.add(
            new NewTopicSpec(
                reg.targetTopic(), partitions, replicationFactor, options.getTopicConfigs()));
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
    int consecutivePublishFailures = 0;
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
      BatchResult result = processBatch(reg, pub, batch);
      if (result.interrupted()) {
        return;
      }
      // A successful publish anywhere in the batch breaks any in-progress
      // failure streak. An empty batch reports both flags false and leaves
      // the counter alone — without this, intermittent traffic during a
      // sustained outage would reset the streak on every empty poll and
      // the breaker would never trip.
      if (result.anyPublishSucceeded()) {
        consecutivePublishFailures = 0;
      }
      if (result.publishFailed()) {
        consecutivePublishFailures++;
        if (consecutivePublishFailures >= config.publishFailureThreshold()) {
          LOG.log(
              Level.WARNING,
              "circuit-breaker engaged for {0} after {1} consecutive publish failures; "
                  + "pausing polling for {2}",
              new Object[] {
                reg.targetTopic(), consecutivePublishFailures, config.publishFailureCooldown()
              });
          try {
            sleeper.sleep(config.publishFailureCooldown());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          }
          // Probe state: after cooldown the very next publish failure trips
          // the breaker again, so a sustained outage adds at most one polled
          // batch of unacked bookkeeping per cooldown cycle (instead of up to
          // `threshold` batches if we reset to zero). A single successful
          // publish drops the counter back to zero on the next iteration.
          consecutivePublishFailures = config.publishFailureThreshold() - 1;
        }
      }
    }
  }

  private BatchResult processBatch(Registration reg, PubSubPublisher pub, List<Message> batch) {
    boolean anyPublishSucceeded = false;
    // Resolve the schema once per batch: stable view across the batch even if a new version is
    // registered mid-iteration, while still picking up registry updates on the next poll.
    Schema schema = resolveSchema(reg);
    for (Message m : batch) {
      if (state != State.Running) {
        return new BatchResult(anyPublishSucceeded, false, false);
      }
      if (schema != null) {
        ValidationResult result = validate(schema, m, reg);
        if (!result.valid()) {
          SchemaViolationPolicy policy = config.schemaViolationPolicy();
          LOG.log(
              Level.WARNING,
              "schema violation on {0} (subject={1} v{2}, policy={3}): {4}",
              new Object[] {
                reg.targetTopic(),
                schema.subject(),
                schema.version(),
                policy,
                result.errors()
              });
          if (policy == SchemaViolationPolicy.FAIL) {
            return new BatchResult(anyPublishSucceeded, true, false);
          }
          // DROP: ack the source so it does not redeliver, then continue with the batch.
          reg.consumer().acknowledge(m);
          continue;
        }
      }
      Message rewritten = MessageRewriter.rewrite(m, reg, schema);
      try {
        pub.publish(rewritten).get(config.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return new BatchResult(anyPublishSucceeded, false, true);
      } catch (ExecutionException | TimeoutException e) {
        LOG.log(
            Level.WARNING,
            "publish failed for " + reg.targetTopic() + "; breaking batch (no ack)",
            e);
        return new BatchResult(anyPublishSucceeded, true, false);
      }
      anyPublishSucceeded = true;
      reg.consumer().acknowledge(m);
    }
    return new BatchResult(anyPublishSucceeded, false, false);
  }

  private Schema resolveSchema(Registration reg) {
    SchemaRegistry registry = config.schemaRegistry();
    if (registry == null) {
      return null;
    }
    Optional<Schema> latest = registry.latest(reg.targetTopic());
    return latest.orElse(null);
  }

  private ValidationResult validate(Schema schema, Message m, Registration reg) {
    SchemaValidator validator = config.schemaValidator();
    try {
      ValidationResult result = validator.validate(schema, m.getPayload());
      if (result == null) {
        return ValidationResult.fail(
            "validator returned null for " + reg.targetTopic());
      }
      return result;
    } catch (RuntimeException e) {
      LOG.log(
          Level.WARNING,
          "schema validator threw for " + reg.targetTopic() + "; treating as violation",
          e);
      return ValidationResult.fail("validator threw: " + e.getMessage());
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
