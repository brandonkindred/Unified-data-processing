package com.unifieddataprocessing.pubsub.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {

  @Mock private Producer<byte[], byte[]> mockKafkaClient;

  private AtomicReference<Properties> capturedProps;
  private KafkaProducerConfig config;
  private KafkaProducer publisher;

  @BeforeEach
  void setUp() {
    capturedProps = new AtomicReference<>();
    config = new KafkaProducerConfig("broker:9092", Map.of("linger.ms", 5));
    publisher =
        new KafkaProducer(
            config,
            props -> {
              capturedProps.set(props);
              return mockKafkaClient;
            });
  }

  @Test
  void connect_appliesFrameworkOverridesAndUserConfig() {
    publisher.connect();

    Properties props = capturedProps.get();
    assertNotNull(props);
    assertEquals("broker:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(5, props.get("linger.ms"));
    assertEquals(
        ByteArraySerializer.class.getName(),
        props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(
        ByteArraySerializer.class.getName(),
        props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    publisher.connect();
    assertThrows(IllegalStateException.class, publisher::connect);
  }

  @Test
  void operationsBeforeConnect_throw() {
    Message m = new Message("id", "t", new byte[0], null);
    assertThrows(IllegalStateException.class, () -> publisher.publish(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishSync(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishBatch(List.of(m)));
    assertThrows(IllegalStateException.class, publisher::flush);
  }

  @Test
  void publish_buildsRecordWithKeyAndHeaders() throws Exception {
    publisher.connect();
    stubSendWithMetadata("topic-a", 0, 7L, 1234L);

    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("x", "1");
    attrs.put(KafkaProducer.ATTR_KEY, "k");
    attrs.put("y", "2");
    Message m = new Message("mid", "topic-a", "payload".getBytes(StandardCharsets.UTF_8), attrs);
    final CompletableFuture<PublishResult> future = publisher.publish(m);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockKafkaClient).send(captor.capture(), any(Callback.class));
    ProducerRecord<byte[], byte[]> sent = captor.getValue();

    assertEquals("topic-a", sent.topic());
    assertArrayEquals("k".getBytes(StandardCharsets.UTF_8), sent.key());
    assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), sent.value());

    Map<String, byte[]> headerMap = new HashMap<>();
    for (Header h : sent.headers()) {
      headerMap.put(h.key(), h.value());
    }
    assertEquals(2, headerMap.size());
    assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), headerMap.get("x"));
    assertArrayEquals("2".getBytes(StandardCharsets.UTF_8), headerMap.get("y"));

    PublishResult result = future.get();
    assertEquals("topic-a", result.getTopic());
    assertEquals(0, result.getPartition());
    assertEquals(7L, result.getOffset());
    assertEquals(1234L, result.getTimestamp());
    assertEquals("mid", result.getMessageId());
  }

  @Test
  void publish_keyAttributeAbsentMeansNullKey() throws Exception {
    publisher.connect();
    stubSendWithMetadata("topic-a", 0, 0L, 0L);

    Message m = new Message("mid", "topic-a", new byte[] {1}, Map.of("h1", "v1"));
    publisher.publish(m).get();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockKafkaClient).send(captor.capture(), any(Callback.class));
    assertNull(captor.getValue().key());
  }

  @Test
  void publish_completesExceptionallyOnSendFailure() {
    publisher.connect();
    KafkaException sendError = new KafkaException("boom");
    when(mockKafkaClient.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            (Answer<java.util.concurrent.Future<RecordMetadata>>)
                invocation -> {
                  Callback cb = invocation.getArgument(1);
                  cb.onCompletion(null, sendError);
                  return null;
                });

    Message m = new Message("mid", "topic-a", new byte[0], null);
    CompletableFuture<PublishResult> cf = publisher.publish(m);
    ExecutionException ee = assertThrows(ExecutionException.class, cf::get);
    assertSame(sendError, ee.getCause());
  }

  @Test
  void publishSync_unwrapsRuntimeException() {
    publisher.connect();
    KafkaException sendError = new KafkaException("boom");
    when(mockKafkaClient.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            (Answer<java.util.concurrent.Future<RecordMetadata>>)
                invocation -> {
                  Callback cb = invocation.getArgument(1);
                  cb.onCompletion(null, sendError);
                  return null;
                });
    Message m = new Message("mid", "topic-a", new byte[0], null);
    KafkaException thrown = assertThrows(KafkaException.class, () -> publisher.publishSync(m));
    assertSame(sendError, thrown);
  }

  @Test
  void publishBatch_emptyReturnsEmptyImmediately() throws Exception {
    publisher.connect();
    List<PublishResult> results = publisher.publishBatch(List.of()).get();
    assertTrue(results.isEmpty());
    verify(mockKafkaClient, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void publishBatch_aggregatesSuccesses() throws Exception {
    publisher.connect();
    stubSendWithMetadata("topic-a", 0, 1L, 100L);

    Message m1 = new Message("a", "topic-a", new byte[] {1}, null);
    Message m2 = new Message("b", "topic-a", new byte[] {2}, null);
    Message m3 = new Message("c", "topic-a", new byte[] {3}, null);

    List<PublishResult> results = publisher.publishBatch(List.of(m1, m2, m3)).get();
    assertEquals(3, results.size());
    assertEquals("a", results.get(0).getMessageId());
    assertEquals("b", results.get(1).getMessageId());
    assertEquals("c", results.get(2).getMessageId());
  }

  @Test
  void publishBatch_aggregatesFailures() {
    publisher.connect();
    KafkaException error = new KafkaException("nope");
    AtomicReference<Integer> callIndex = new AtomicReference<>(0);
    when(mockKafkaClient.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            (Answer<java.util.concurrent.Future<RecordMetadata>>)
                invocation -> {
                  int idx = callIndex.get();
                  callIndex.set(idx + 1);
                  Callback cb = invocation.getArgument(1);
                  ProducerRecord<?, ?> rec = invocation.getArgument(0);
                  if (idx == 1) {
                    cb.onCompletion(null, error);
                  } else {
                    cb.onCompletion(
                        new RecordMetadata(new TopicPartition(rec.topic(), 0), 0L, 0, 0L, 0, 0),
                        null);
                  }
                  return null;
                });

    Message m1 = new Message("a", "topic-a", new byte[] {1}, null);
    Message m2 = new Message("b", "topic-a", new byte[] {2}, null);
    Message m3 = new Message("c", "topic-a", new byte[] {3}, null);

    CompletableFuture<List<PublishResult>> batch = publisher.publishBatch(List.of(m1, m2, m3));
    ExecutionException ee = assertThrows(ExecutionException.class, batch::get);
    PublishBatchException pbe = (PublishBatchException) ee.getCause();
    assertEquals(2, pbe.getSucceeded().size());
    assertEquals(1, pbe.getFailures().size());
    assertSame(error, pbe.getFailures().get(1));
    assertSame(error, pbe.getCause());
  }

  @Test
  void flush_callsProducerFlush() {
    publisher.connect();
    publisher.flush();
    verify(mockKafkaClient).flush();
  }

  @Test
  void close_closesProducerAndIsIdempotent() {
    publisher.connect();
    publisher.close();
    publisher.close();
    verify(mockKafkaClient, org.mockito.Mockito.times(1)).close();
  }

  private void stubSendWithMetadata(String topic, int partition, long offset, long timestamp) {
    when(mockKafkaClient.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            (Answer<java.util.concurrent.Future<RecordMetadata>>)
                invocation -> {
                  Callback cb = invocation.getArgument(1);
                  cb.onCompletion(
                      new RecordMetadata(
                          new TopicPartition(topic, partition), offset, 0, timestamp, 0, 0),
                      null);
                  return null;
                });
  }
}
