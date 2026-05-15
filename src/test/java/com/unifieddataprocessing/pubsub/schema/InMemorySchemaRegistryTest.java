package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.unifieddataprocessing.pubsub.Message;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class InMemorySchemaRegistryTest {

  private static final Schema NOOP = message -> {};
  private static final Schema OTHER = message -> {};

  @Test
  void register_thenFind_returnsSchema() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    registry.register("orders", NOOP);

    Optional<Schema> found = registry.findSchema("orders");
    assertTrue(found.isPresent());
    assertSame(NOOP, found.get());
  }

  @Test
  void findSchema_unknownTopic_returnsEmpty() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    assertTrue(registry.findSchema("nope").isEmpty());
  }

  @Test
  void unregister_removesSchema() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    registry.register("orders", NOOP);
    registry.unregister("orders");
    assertTrue(registry.findSchema("orders").isEmpty());
  }

  @Test
  void unregister_unknownTopic_isNoOp() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    registry.unregister("never-registered");
    assertTrue(registry.listTopics().isEmpty());
  }

  @Test
  void register_overwritesOnReRegister() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    registry.register("orders", NOOP);
    registry.register("orders", OTHER);

    assertSame(OTHER, registry.findSchema("orders").orElseThrow());
    assertEquals(1, registry.listTopics().size());
  }

  @Test
  void register_nullTopic_throwsNpe() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    assertThrows(NullPointerException.class, () -> registry.register(null, NOOP));
  }

  @Test
  void register_nullSchema_throwsNpe() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    assertThrows(NullPointerException.class, () -> registry.register("orders", null));
  }

  @Test
  void unregister_nullTopic_throwsNpe() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    assertThrows(NullPointerException.class, () -> registry.unregister(null));
  }

  @Test
  void findSchema_nullTopic_throwsNpe() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    assertThrows(NullPointerException.class, () -> registry.findSchema(null));
  }

  @Test
  void listTopics_returnsImmutableSnapshot() {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    registry.register("a", NOOP);
    registry.register("b", NOOP);
    registry.register("c", NOOP);

    Set<String> snapshot = registry.listTopics();
    assertEquals(Set.of("a", "b", "c"), snapshot);

    registry.unregister("a");
    assertTrue(snapshot.contains("a"), "prior snapshot must not reflect later mutations");
    assertFalse(registry.listTopics().contains("a"));

    assertThrows(UnsupportedOperationException.class, () -> snapshot.add("d"));
  }

  @Test
  void register_concurrent32Threads_allVisible() throws InterruptedException {
    InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
    int threadCount = 32;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch doneGate = new CountDownLatch(threadCount);

    try {
      for (int i = 0; i < threadCount; i++) {
        final String topic = "topic-" + i;
        pool.submit(
            () -> {
              try {
                startGate.await();
                registry.register(topic, sentinel(topic));
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              } finally {
                doneGate.countDown();
              }
            });
      }
      startGate.countDown();
      assertTrue(doneGate.await(5, TimeUnit.SECONDS), "registrations did not complete in time");
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    Set<String> expected = new HashSet<>();
    for (int i = 0; i < threadCount; i++) {
      expected.add("topic-" + i);
    }
    assertEquals(expected, registry.listTopics());
    for (String topic : expected) {
      assertTrue(registry.findSchema(topic).isPresent(), "missing " + topic);
    }
  }

  private static Schema sentinel(String name) {
    return new Schema() {
      @Override
      public void validate(Message message) {
        // marker schema; identity per topic is unimportant for the visibility check
      }

      @Override
      public String toString() {
        return "sentinel(" + name + ")";
      }
    };
  }
}
