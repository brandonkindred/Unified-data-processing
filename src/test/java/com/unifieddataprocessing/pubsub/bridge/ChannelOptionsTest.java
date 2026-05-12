package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChannelOptionsTest {

  @Test
  void defaults_returnsZeroPartitionsAndReplicationAndEmptyMap() {
    ChannelOptions opts = ChannelOptions.defaults();
    assertEquals(0, opts.getPartitions());
    assertEquals((short) 0, opts.getReplicationFactor());
    assertTrue(opts.getTopicConfigs().isEmpty());
  }

  @Test
  void builder_defaults_matchDefaultsFactory() {
    ChannelOptions built = ChannelOptions.builder().build();
    ChannelOptions factory = ChannelOptions.defaults();
    assertEquals(factory.getPartitions(), built.getPartitions());
    assertEquals(factory.getReplicationFactor(), built.getReplicationFactor());
    assertEquals(factory.getTopicConfigs(), built.getTopicConfigs());
  }

  @Test
  void builder_negativePartitionsRejected() {
    assertThrows(IllegalArgumentException.class, () -> ChannelOptions.builder().partitions(-1));
  }

  @Test
  void builder_negativeReplicationFactorRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ChannelOptions.builder().replicationFactor((short) -1));
  }

  @Test
  void builder_zeroPartitionsAccepted() {
    ChannelOptions opts = ChannelOptions.builder().partitions(0).build();
    assertEquals(0, opts.getPartitions());
  }

  @Test
  void builder_zeroReplicationFactorAccepted() {
    ChannelOptions opts = ChannelOptions.builder().replicationFactor((short) 0).build();
    assertEquals((short) 0, opts.getReplicationFactor());
  }

  @Test
  void builder_positiveValuesAccepted() {
    ChannelOptions opts =
        ChannelOptions.builder().partitions(6).replicationFactor((short) 3).build();
    assertEquals(6, opts.getPartitions());
    assertEquals((short) 3, opts.getReplicationFactor());
  }

  @Test
  void builder_nullTopicConfigsRejected() {
    assertThrows(
        NullPointerException.class, () -> ChannelOptions.builder().topicConfigs(null));
  }

  @Test
  void builder_nullTopicConfigKeyRejected() {
    assertThrows(
        NullPointerException.class, () -> ChannelOptions.builder().topicConfig(null, "v"));
  }

  @Test
  void builder_nullTopicConfigValueRejected() {
    assertThrows(
        NullPointerException.class, () -> ChannelOptions.builder().topicConfig("k", null));
  }

  @Test
  void topicConfigs_returnedMapIsUnmodifiable() {
    ChannelOptions opts =
        ChannelOptions.builder().topicConfig("retention.ms", "604800000").build();
    Map<String, String> view = opts.getTopicConfigs();
    assertThrows(UnsupportedOperationException.class, () -> view.put("k", "v"));
  }

  @Test
  void topicConfig_singleEntryAccumulates() {
    ChannelOptions opts =
        ChannelOptions.builder()
            .topicConfig("retention.ms", "604800000")
            .topicConfig("cleanup.policy", "compact")
            .build();
    Map<String, String> cfg = opts.getTopicConfigs();
    assertEquals(2, cfg.size());
    assertEquals("604800000", cfg.get("retention.ms"));
    assertEquals("compact", cfg.get("cleanup.policy"));
  }

  @Test
  void topicConfigs_bulkSetterCopiesEntries() {
    Map<String, String> source = new HashMap<>();
    source.put("retention.ms", "1000");
    ChannelOptions opts = ChannelOptions.builder().topicConfigs(source).build();
    source.put("retention.ms", "9999");
    assertEquals("1000", opts.getTopicConfigs().get("retention.ms"));
  }
}
