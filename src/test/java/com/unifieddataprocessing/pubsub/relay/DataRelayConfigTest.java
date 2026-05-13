package com.unifieddataprocessing.pubsub.relay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DataRelayConfigTest {

  @Test
  void build_default_appliesAllDefaults() {
    DataRelayConfig cfg = DataRelayConfig.builder().build();

    assertEquals(Duration.ofSeconds(1), cfg.pollTimeout());
    assertEquals(Duration.ofSeconds(30), cfg.publishTimeout());
    assertEquals(Duration.ofSeconds(30), cfg.shutdownTimeout());
    assertEquals(Duration.ofSeconds(5), cfg.closeForceTimeout());
    assertEquals(Duration.ofSeconds(1), cfg.pollBackoff());
  }

  @Test
  void pollTimeout_null_throws() {
    assertThrows(NullPointerException.class, () -> DataRelayConfig.builder().pollTimeout(null));
  }

  @Test
  void pollTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().pollTimeout(Duration.ZERO));
  }

  @Test
  void pollTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().pollTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void publishTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataRelayConfig.builder().publishTimeout(null));
  }

  @Test
  void publishTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().publishTimeout(Duration.ZERO));
  }

  @Test
  void publishTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().publishTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void shutdownTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataRelayConfig.builder().shutdownTimeout(null));
  }

  @Test
  void shutdownTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().shutdownTimeout(Duration.ZERO));
  }

  @Test
  void shutdownTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().shutdownTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void closeForceTimeout_null_throws() {
    assertThrows(
        NullPointerException.class, () -> DataRelayConfig.builder().closeForceTimeout(null));
  }

  @Test
  void closeForceTimeout_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().closeForceTimeout(Duration.ZERO));
  }

  @Test
  void closeForceTimeout_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().closeForceTimeout(Duration.ofMillis(-1)));
  }

  @Test
  void pollBackoff_null_throws() {
    assertThrows(NullPointerException.class, () -> DataRelayConfig.builder().pollBackoff(null));
  }

  @Test
  void pollBackoff_zero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().pollBackoff(Duration.ZERO));
  }

  @Test
  void pollBackoff_negative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DataRelayConfig.builder().pollBackoff(Duration.ofMillis(-1)));
  }

  @Test
  void build_allFieldsOverridden_returnsExactValues() {
    DataRelayConfig cfg =
        DataRelayConfig.builder()
            .pollTimeout(Duration.ofMillis(250))
            .publishTimeout(Duration.ofSeconds(10))
            .shutdownTimeout(Duration.ofSeconds(15))
            .closeForceTimeout(Duration.ofSeconds(2))
            .pollBackoff(Duration.ofMillis(500))
            .build();

    assertEquals(Duration.ofMillis(250), cfg.pollTimeout());
    assertEquals(Duration.ofSeconds(10), cfg.publishTimeout());
    assertEquals(Duration.ofSeconds(15), cfg.shutdownTimeout());
    assertEquals(Duration.ofSeconds(2), cfg.closeForceTimeout());
    assertEquals(Duration.ofMillis(500), cfg.pollBackoff());
  }
}
