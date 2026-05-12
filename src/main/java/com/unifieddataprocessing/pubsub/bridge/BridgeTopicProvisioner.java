package com.unifieddataprocessing.pubsub.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Provisions Kafka topics required by the data bridge.
 *
 * <p>{@link #provision(Properties, List)} creates an {@link AdminClient} from the injected factory,
 * issues a single {@code createTopics} batch for the given specs, and inspects each per-topic
 * future. {@link TopicExistsException} is swallowed (idempotent first-run behavior); any other
 * failure is rethrown. The {@code AdminClient} is closed in a {@code finally} block so it is
 * always released — including on the rethrow path.
 */
final class BridgeTopicProvisioner {

  private final Function<Properties, AdminClient> adminFactory;

  BridgeTopicProvisioner(Function<Properties, AdminClient> adminFactory) {
    this.adminFactory = Objects.requireNonNull(adminFactory, "adminFactory");
  }

  /**
   * Attempts to create every topic in {@code specs}. Swallows {@link TopicExistsException} per
   * topic; rethrows any other failure (with the {@link AdminClient} still closed via try/finally).
   *
   * @param adminProps Kafka admin client properties (e.g. {@code bootstrap.servers})
   * @param specs topics to create; may be empty
   */
  void provision(Properties adminProps, List<NewTopicSpec> specs) {
    Objects.requireNonNull(adminProps, "adminProps");
    Objects.requireNonNull(specs, "specs");

    AdminClient admin = adminFactory.apply(adminProps);
    try {
      List<NewTopic> topics = new ArrayList<>(specs.size());
      for (NewTopicSpec spec : specs) {
        NewTopic nt = new NewTopic(spec.name(), spec.partitions(), spec.replicationFactor());
        nt.configs(spec.configs());
        topics.add(nt);
      }

      CreateTopicsResult result = admin.createTopics(topics);
      for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
        try {
          entry.getValue().get();
        } catch (ExecutionException ee) {
          Throwable cause = ee.getCause();
          if (cause instanceof TopicExistsException) {
            continue;
          }
          if (cause instanceof RuntimeException re) {
            throw re;
          }
          throw new RuntimeException(cause == null ? ee : cause);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    } finally {
      admin.close();
    }
  }
}
