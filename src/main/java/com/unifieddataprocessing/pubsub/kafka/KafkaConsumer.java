package com.unifieddataprocessing.pubsub.kafka;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PubSubConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Kafka-backed {@link PubSubConsumer}. Wraps {@code org.apache.kafka.clients.consumer.KafkaConsumer}
 * with raw byte payloads and manual commit. Not thread-safe — the underlying Kafka client
 * may only be used from a single thread, and so may instances of this class.
 */
public class KafkaConsumer implements PubSubConsumer {

    private final KafkaConsumerConfig config;
    private final Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
    private final Set<String> subscribedTopics = new LinkedHashSet<>();
    private final Map<String, TopicPartition> partitionByMessageId = new HashMap<>();
    private final Map<String, Long> offsetByMessageId = new HashMap<>();
    // Per-partition watermark state. We track the actual delivered offsets
    // (rather than walking the integers between min and max) because Kafka
    // offsets can have legitimate gaps — compacted topics drop tombstones,
    // transactional topics consume offsets for control records, etc. Walking
    // delivered offsets makes the ack watermark gap-tolerant.
    private final Map<TopicPartition, NavigableSet<Long>> deliveredOffsetsByPartition = new HashMap<>();
    private final Map<TopicPartition, NavigableSet<Long>> ackedOffsetsByPartition = new HashMap<>();

    private Consumer<byte[], byte[]> consumer;

    public KafkaConsumer(KafkaConsumerConfig config) {
        this(config, props -> new org.apache.kafka.clients.consumer.KafkaConsumer<>(props));
    }

    KafkaConsumer(KafkaConsumerConfig config, Function<Properties, Consumer<byte[], byte[]>> consumerFactory) {
        this.config = Objects.requireNonNull(config, "config");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
    }

    @Override
    public void connect() {
        if (consumer != null) {
            throw new IllegalStateException("already connected");
        }
        Properties props = config.toProperties();
        // Framework controls deserialization (raw bytes) and commit semantics.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = consumerFactory.apply(props);
    }

    @Override
    public void subscribe(String topic) {
        Objects.requireNonNull(topic, "topic");
        ensureConnected();
        if (subscribedTopics.add(topic)) {
            consumer.subscribe(new LinkedHashSet<>(subscribedTopics));
        }
    }

    @Override
    public void unsubscribe(String topic) {
        Objects.requireNonNull(topic, "topic");
        ensureConnected();
        if (!subscribedTopics.remove(topic)) {
            return;
        }
        if (subscribedTopics.isEmpty()) {
            consumer.unsubscribe();
        } else {
            consumer.subscribe(new LinkedHashSet<>(subscribedTopics));
        }
    }

    @Override
    public List<Message> poll(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        ensureConnected();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        List<Message> result = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            String id = record.topic() + "-" + record.partition() + "-" + record.offset();
            Map<String, String> attributes = new LinkedHashMap<>();
            record.headers().forEach(h -> attributes.put(
                    h.key(),
                    h.value() == null ? "" : new String(h.value(), StandardCharsets.UTF_8)));
            byte[] payload = record.value() == null ? new byte[0] : record.value();
            Message message = new Message(id, record.topic(), payload, attributes);
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            partitionByMessageId.put(id, tp);
            offsetByMessageId.put(id, record.offset());
            deliveredOffsetsByPartition.computeIfAbsent(tp, k -> new TreeSet<>()).add(record.offset());
            result.add(message);
        }
        return result;
    }

    @Override
    public void acknowledge(Message message) {
        Objects.requireNonNull(message, "message");
        ensureConnected();
        TopicPartition tp = partitionByMessageId.get(message.getId());
        Long offset = offsetByMessageId.get(message.getId());
        if (tp == null || offset == null) {
            throw new IllegalStateException(
                    "Unknown message: " + message.getId() + ". Only messages returned by poll() can be acknowledged.");
        }

        // Find the longest fully-acked prefix of *delivered* offsets on this
        // partition. Walking delivered offsets (instead of integer-stepping)
        // tolerates legitimate offset gaps from compaction or transactional
        // control records. We only commit when the prefix advances; on commit
        // success we drain the prefix from both delivered and acked, so a
        // commitSync failure leaves all state intact for retry.
        NavigableSet<Long> delivered = deliveredOffsetsByPartition.computeIfAbsent(tp, k -> new TreeSet<>());
        NavigableSet<Long> acked = ackedOffsetsByPartition.computeIfAbsent(tp, k -> new TreeSet<>());
        acked.add(offset);

        Long highestAckedDelivered = null;
        for (Long delivOff : delivered) {
            if (acked.contains(delivOff)) {
                highestAckedDelivered = delivOff;
            } else {
                break;
            }
        }

        if (highestAckedDelivered != null) {
            long commitOffset = highestAckedDelivered + 1;
            consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(commitOffset)));
            delivered.headSet(highestAckedDelivered, true).clear();
            acked.headSet(highestAckedDelivered, true).clear();
        }

        partitionByMessageId.remove(message.getId());
        offsetByMessageId.remove(message.getId());
    }

    @Override
    public void close() {
        if (consumer == null) {
            return;
        }
        try {
            consumer.close();
        } finally {
            consumer = null;
            subscribedTopics.clear();
            partitionByMessageId.clear();
            offsetByMessageId.clear();
            deliveredOffsetsByPartition.clear();
            ackedOffsetsByPartition.clear();
        }
    }

    private void ensureConnected() {
        if (consumer == null) {
            throw new IllegalStateException("not connected; call connect() first");
        }
    }
}
