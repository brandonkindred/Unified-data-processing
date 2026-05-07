package com.unifieddataprocessing.pubsub.kafka;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubPublisher;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Kafka-backed {@link PubSubPublisher}. Wraps {@code
 * org.apache.kafka.clients.producer.KafkaProducer} with raw byte payloads. Topic is read from
 * {@link Message#getTopic()} on every publish.
 *
 * <p>The framework {@link Message} has no key field; callers that need a Kafka record key may set
 * the reserved attribute {@link #ATTR_KEY} on the message — its value is UTF-8-encoded into the
 * record key. All other attributes become record headers (UTF-8-encoded), mirroring how {@code
 * KafkaConsumer.poll} surfaces them on the read path.
 *
 * <p>Not thread-safe — although the underlying Kafka producer is thread-safe, the per-instance
 * "connected" bookkeeping in this wrapper is not.
 */
public class KafkaProducer implements PubSubPublisher {

  /** Reserved {@link Message} attribute key whose UTF-8 value becomes the Kafka record key. */
  public static final String ATTR_KEY = "kafkaKey";

  private final KafkaProducerConfig config;
  private final Function<Properties, Producer<byte[], byte[]>> producerFactory;

  private Producer<byte[], byte[]> producer;

  public KafkaProducer(KafkaProducerConfig config) {
    this(config, props -> new org.apache.kafka.clients.producer.KafkaProducer<>(props));
  }

  KafkaProducer(
      KafkaProducerConfig config, Function<Properties, Producer<byte[], byte[]>> producerFactory) {
    this.config = Objects.requireNonNull(config, "config");
    this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
  }

  @Override
  public void connect() {
    if (producer != null) {
      throw new IllegalStateException("already connected");
    }
    Properties props = config.toProperties();
    // Framework controls serialization (raw bytes). Set last so a caller-supplied
    // override in extras cannot break the byte[] payload contract.
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = producerFactory.apply(props);
  }

  @Override
  public CompletableFuture<PublishResult> publish(Message message) {
    Objects.requireNonNull(message, "message");
    ensureConnected();
    return doPublish(message);
  }

  @Override
  public PublishResult publishSync(Message message) {
    try {
      return publish(message).join();
    } catch (CompletionException e) {
      throw unwrap(e);
    }
  }

  @Override
  public CompletableFuture<List<PublishResult>> publishBatch(List<Message> messages) {
    Objects.requireNonNull(messages, "messages");
    ensureConnected();
    if (messages.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    List<CompletableFuture<PublishResult>> perMessage = new ArrayList<>(messages.size());
    for (Message m : messages) {
      Objects.requireNonNull(m, "messages contains null");
      perMessage.add(doPublish(m));
    }
    return aggregate(perMessage);
  }

  @Override
  public void flush() {
    ensureConnected();
    producer.flush();
  }

  @Override
  public void close() {
    if (producer == null) {
      return;
    }
    try {
      producer.close();
    } finally {
      producer = null;
    }
  }

  private CompletableFuture<PublishResult> doPublish(Message message) {
    Map<String, String> attrs = message.getAttributes();
    String keyAttr = attrs.get(ATTR_KEY);
    byte[] key = keyAttr == null ? null : keyAttr.getBytes(StandardCharsets.UTF_8);
    RecordHeaders headers = new RecordHeaders();
    for (Map.Entry<String, String> e : attrs.entrySet()) {
      if (ATTR_KEY.equals(e.getKey())) {
        continue;
      }
      headers.add(
          new RecordHeader(
              e.getKey(),
              e.getValue() == null ? null : e.getValue().getBytes(StandardCharsets.UTF_8)));
    }
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(
            message.getTopic(), null, null, key, message.getPayload(), headers);
    CompletableFuture<PublishResult> cf = new CompletableFuture<>();
    String fallbackId = message.getId();
    producer.send(
        record,
        (md, ex) -> {
          if (ex != null) {
            cf.completeExceptionally(ex);
            return;
          }
          cf.complete(
              PublishResult.forKafka(
                  md.topic(), fallbackId, md.partition(), md.offset(), md.timestamp()));
        });
    return cf;
  }

  /**
   * Aggregate-not-fail-fast batch combiner. Waits for every per-message future, then emits either a
   * full success list or a {@link PublishBatchException} carrying the partial successes plus the
   * per-index failure map.
   */
  private static CompletableFuture<List<PublishResult>> aggregate(
      List<CompletableFuture<PublishResult>> perMessage) {
    CompletableFuture<Void> all =
        CompletableFuture.allOf(perMessage.toArray(new CompletableFuture<?>[0]));
    return all.handle(
        (v, t) -> {
          List<PublishResult> succeeded = new ArrayList<>();
          Map<Integer, Throwable> failures = new LinkedHashMap<>();
          for (int i = 0; i < perMessage.size(); i++) {
            CompletableFuture<PublishResult> f = perMessage.get(i);
            if (f.isCompletedExceptionally()) {
              try {
                f.join();
              } catch (CompletionException ce) {
                failures.put(i, ce.getCause() == null ? ce : ce.getCause());
              } catch (RuntimeException re) {
                failures.put(i, re);
              }
            } else {
              succeeded.add(f.join());
            }
          }
          if (failures.isEmpty()) {
            return succeeded;
          }
          throw new PublishBatchException(succeeded, failures);
        });
  }

  private static RuntimeException unwrap(CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException re) {
      return re;
    }
    if (cause == null) {
      return e;
    }
    return new RuntimeException(cause);
  }

  private void ensureConnected() {
    if (producer == null) {
      throw new IllegalStateException("not connected; call connect() first");
    }
  }
}
