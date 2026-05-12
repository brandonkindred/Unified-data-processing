package com.unifieddataprocessing.pubsub.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BridgeTopicProvisionerTest {

  @Mock private AdminClient adminClient;
  @Mock private CreateTopicsResult createTopicsResult;

  private final Function<Properties, AdminClient> adminFactory = props -> adminClient;

  @Test
  void provision_allFuturesSucceed_closesAdmin() {
    Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
    futures.put("a.orders", KafkaFuture.completedFuture(null));
    futures.put("b.leads", KafkaFuture.completedFuture(null));
    when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
    when(createTopicsResult.values()).thenReturn(futures);

    BridgeTopicProvisioner provisioner = new BridgeTopicProvisioner(adminFactory);
    provisioner.provision(
        new Properties(),
        List.of(
            new NewTopicSpec("a.orders", 6, (short) 3, Map.of("retention.ms", "604800000")),
            new NewTopicSpec("b.leads", 1, (short) 1, Map.of())));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<java.util.Collection<NewTopic>> captor =
        ArgumentCaptor.forClass(java.util.Collection.class);
    verify(adminClient).createTopics(captor.capture());
    List<NewTopic> sent = new ArrayList<>(captor.getValue());
    assertEquals(2, sent.size());
    assertEquals("a.orders", sent.get(0).name());
    assertEquals(6, sent.get(0).numPartitions());
    assertEquals((short) 3, sent.get(0).replicationFactor());
    assertEquals(Map.of("retention.ms", "604800000"), sent.get(0).configs());
    assertEquals("b.leads", sent.get(1).name());
    assertEquals(1, sent.get(1).numPartitions());
    assertEquals((short) 1, sent.get(1).replicationFactor());

    verify(adminClient).close();
  }

  @Test
  void provision_oneTopicExistsException_swallowsAndClosesAdmin() {
    KafkaFutureImpl<Void> existsFuture = new KafkaFutureImpl<>();
    existsFuture.completeExceptionally(new TopicExistsException("a.orders already exists"));

    Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
    futures.put("a.orders", existsFuture);
    futures.put("b.leads", KafkaFuture.completedFuture(null));
    when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
    when(createTopicsResult.values()).thenReturn(futures);

    BridgeTopicProvisioner provisioner = new BridgeTopicProvisioner(adminFactory);
    // Must not throw.
    provisioner.provision(
        new Properties(),
        List.of(
            new NewTopicSpec("a.orders", 1, (short) 1, Map.of()),
            new NewTopicSpec("b.leads", 1, (short) 1, Map.of())));

    verify(adminClient).close();
  }

  @Test
  void provision_authorizationException_rethrowsAndStillClosesAdmin() {
    KafkaFutureImpl<Void> authFuture = new KafkaFutureImpl<>();
    TopicAuthorizationException authError =
        new TopicAuthorizationException("not allowed to create a.orders");
    authFuture.completeExceptionally(authError);

    Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
    futures.put("a.orders", authFuture);
    when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
    when(createTopicsResult.values()).thenReturn(futures);

    BridgeTopicProvisioner provisioner = new BridgeTopicProvisioner(adminFactory);
    TopicAuthorizationException thrown =
        assertThrows(
            TopicAuthorizationException.class,
            () ->
                provisioner.provision(
                    new Properties(),
                    List.of(new NewTopicSpec("a.orders", 1, (short) 1, Map.of()))));
    assertEquals(authError.getMessage(), thrown.getMessage());

    verify(adminClient).close();
  }
}
