package com.unifieddataprocessing.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class InMemorySchemaRegistryTest {

  @Test
  void register_firstTime_startsAtVersionOne() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    Schema s = reg.register("orders", "JSON", "{\"v\":1}");
    assertEquals("orders", s.subject());
    assertEquals(1, s.version());
    assertEquals("JSON", s.type());
    assertEquals("{\"v\":1}", s.definition());
  }

  @Test
  void register_bumpsVersionMonotonically() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    Schema v1 = reg.register("orders", "JSON", "{\"v\":1}");
    Schema v2 = reg.register("orders", "JSON", "{\"v\":2}");
    Schema v3 = reg.register("orders", "AVRO", "{\"v\":3}");
    assertEquals(1, v1.version());
    assertEquals(2, v2.version());
    assertEquals(3, v3.version());
    assertEquals("AVRO", v3.type());
  }

  @Test
  void latest_unknownSubject_returnsEmpty() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    assertEquals(Optional.empty(), reg.latest("nope"));
  }

  @Test
  void latest_returnsHighestVersion() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    reg.register("orders", "JSON", "v2");
    Schema latest = reg.latest("orders").orElseThrow();
    assertEquals(2, latest.version());
    assertEquals("v2", latest.definition());
  }

  @Test
  void get_exactVersion_returnsThatSchema() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    reg.register("orders", "JSON", "v2");
    Schema v1 = reg.get("orders", 1);
    assertEquals(1, v1.version());
    assertEquals("v1", v1.definition());
  }

  @Test
  void get_unknownSubject_throws() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    assertThrows(SubjectNotFoundException.class, () -> reg.get("nope", 1));
  }

  @Test
  void get_unknownVersion_throws() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    assertThrows(VersionNotFoundException.class, () -> reg.get("orders", 2));
  }

  @Test
  void get_versionZero_throws() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    assertThrows(IllegalArgumentException.class, () -> reg.get("orders", 0));
  }

  @Test
  void versions_unknownSubject_returnsEmpty() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    assertEquals(List.of(), reg.versions("nope"));
  }

  @Test
  void versions_returnsAscendingOrder() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    reg.register("orders", "JSON", "v2");
    reg.register("orders", "JSON", "v3");
    assertEquals(List.of(1, 2, 3), reg.versions("orders"));
  }

  @Test
  void subjects_listsAllKnown() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    reg.register("payments", "JSON", "v1");
    assertEquals(Set.of("orders", "payments"), reg.subjects());
  }

  @Test
  void deleteSubject_removesAllVersions() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    reg.register("orders", "JSON", "v1");
    reg.register("orders", "JSON", "v2");
    assertTrue(reg.deleteSubject("orders"));
    assertEquals(Optional.empty(), reg.latest("orders"));
    assertEquals(List.of(), reg.versions("orders"));
    assertFalse(reg.subjects().contains("orders"));
  }

  @Test
  void deleteSubject_unknown_returnsFalse() {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    assertFalse(reg.deleteSubject("nope"));
  }

  @Test
  void register_concurrentSameSubject_versionsAreContiguousAndUnique() throws Exception {
    InMemorySchemaRegistry reg = new InMemorySchemaRegistry();
    int threads = 16;
    int perThread = 50;
    int total = threads * perThread;

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Schema> results = java.util.Collections.synchronizedList(new ArrayList<>(total));
    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            try {
              start.await();
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              return;
            }
            for (int i = 0; i < perThread; i++) {
              results.add(reg.register("orders", "JSON", "body"));
            }
          });
    }
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals(total, results.size());
    Set<Integer> seen = new HashSet<>();
    for (Schema s : results) {
      assertTrue(seen.add(s.version()), "duplicate version: " + s.version());
    }
    assertEquals(total, seen.size());
    for (int v = 1; v <= total; v++) {
      assertTrue(seen.contains(v), "missing version: " + v);
    }
    assertEquals(total, reg.latest("orders").orElseThrow().version());
  }
}
