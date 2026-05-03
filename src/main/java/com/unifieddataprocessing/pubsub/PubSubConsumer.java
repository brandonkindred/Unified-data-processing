package com.unifieddataprocessing.pubsub;

import java.time.Duration;
import java.util.List;

public interface PubSubConsumer extends AutoCloseable {

    void connect();

    void subscribe(String topic);

    void unsubscribe(String topic);

    List<Message> poll(Duration timeout);

    void acknowledge(Message message);

    @Override
    void close();
}
