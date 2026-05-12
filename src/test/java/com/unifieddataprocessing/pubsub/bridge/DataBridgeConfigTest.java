package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DataBridgeConfigTest {

  private static final KafkaProducerConfig PRODUCER = new KafkaProducerConfig("kafka:9092");

  @Test
  void build_withOnlyProducerConfig_appliesAllDefaults() {
    DataBridgeConfig cfg = DataBridgeConfig.builder().producerConfig(PRODUCER).build();

    assertSame(PRODUCER, cfg.producerConfig());
    assertEquals(Duration.ofSeconds(1), cfg.pollTimeout());
    assertEquals(Duration.ofSeconds(30), cfg.publishTimeout());
    assertEquals(Duration.ofSeconds(30), cfg.shutdownTimeout());
    assertEquals(Duration.ofSeconds(5), cfg.closeForceTimeout());
    assertEquals(Duration.ofSeconds(1), cfg.pollBackoff());
    assertEquals(1, cfg.defaultPartitions());
    assertEquals((short) 1, cfg.defaultReplicationFactor());
  }

  @Test
  void build_withoutProducerConfig_throws() {
    assertThrows(IllegalStateException.class, () -> DataBridgeConfig.builder().build());
  }

  @Test
  void producerConfig_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataBridgeConfig.builder().producerConfig(null));
  }

  @Test
  void pollTimeout_null_throws() {
    assertThrows(NullPointerException.class, () -> DataBridgeConfig.builder().pollTimeout(null));
  }

  @Test
  void pollTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().pollTimeout(Duration.ZERO));
  }

  @Test
  void pollTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().pollTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void publishTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataBridgeConfig.builder().publishTimeout(null));
  }

  @Test
  void publishTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().publishTimeout(Duration.ZERO));
  }

  @Test
  void publishTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().publishTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void shutdownTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataBridgeConfig.builder().shutdownTimeout(null));
  }

  @Test
  void shutdownTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().shutdownTimeout(Duration.ZERO));
  }

  @Test
  void shutdownTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().shutdownTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void closeForceTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataBridgeConfig.builder().closeForceTimeout(null));
  }

  @Test
  void closeForceTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().closeForceTimeout(Duration.ZERO));
  }

  @Test
  void closeForceTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().closeForceTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void pollBackoff_null_throws() {
    assertThrows(NullPointerException.class, () -> DataBridgeConfig.builder().pollBackoff(null));
  }

  @Test
  void pollBackoff_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().pollBackoff(Duration.ZERO));
  }

  @Test
  void pollBackoff_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().pollBackoff(Duration.ofMillis(-1)));
  }

  @Test
  void defaultPartitions_zero_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> DataBridgeConfig.builder().defaultPartitions(0));
  }

  @Test
  void defaultPartitions_negative_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> DataBridgeConfig.builder().defaultPartitions(-1));
  }

  @Test
  void defaultReplicationFactor_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().defaultReplicationFactor((short) 0));
  }

  @Test
  void defaultReplicationFactor_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataBridgeConfig.builder().defaultReplicationFactor((short) -1));
  }

  @Test
  void build_allFieldsOverridden_returnsExactValues() {
    KafkaProducerConfig producer = new KafkaProducerConfig("kafka:19092");
    DataBridgeConfig cfg =
        DataBridgeConfig.builder()
            .producerConfig(producer)
            .pollTimeout(Duration.ofMillis(250))
            .publishTimeout(Duration.ofSeconds(10))
            .shutdownTimeout(Duration.ofSeconds(15))
            .closeForceTimeout(Duration.ofSeconds(2))
            .pollBackoff(Duration.ofMillis(500))
            .defaultPartitions(6)
            .defaultReplicationFactor((short) 3)
            .build();

    assertSame(producer, cfg.producerConfig());
    assertEquals(Duration.ofMillis(250), cfg.pollTimeout());
    assertEquals(Duration.ofSeconds(10), cfg.publishTimeout());
    assertEquals(Duration.ofSeconds(15), cfg.shutdownTimeout());
    assertEquals(Duration.ofSeconds(2), cfg.closeForceTimeout());
    assertEquals(Duration.ofMillis(500), cfg.pollBackoff());
    assertEquals(6, cfg.defaultPartitions());
    assertEquals((short) 3, cfg.defaultReplicationFactor());
  }
}
