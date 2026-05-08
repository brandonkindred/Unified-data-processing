package com.unifieddataprocessing.pubsub.kinesis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.PublishBatchException;
import com.unifieddataprocessing.pubsub.PublishResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

@ExtendWith(MockitoExtension.class)
class KinesisPublisherTest {

  @Mock private KinesisClient mockClient;

  private AtomicReference<KinesisPublisherConfig> capturedClientConfig;
  private KinesisPublisher publisher;

  @BeforeEach
  void setUp() {
    capturedClientConfig = new AtomicReference<>();
    KinesisPublisherConfig config =
        new KinesisPublisherConfig(
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")));
    publisher =
        new KinesisPublisher(
            config,
            c -> {
              capturedClientConfig.set(c);
              return mockClient;
            },
            c -> directExecutor());
  }

  @Test
  void connect_buildsClientFromFactory() {
    publisher.connect();
    assertNotNull(capturedClientConfig.get());
    assertEquals(Region.US_EAST_1, capturedClientConfig.get().getRegion());
  }

  @Test
  void connect_throwsIfAlreadyConnected() {
    publisher.connect();
    assertThrows(IllegalStateException.class, publisher::connect);
  }

  @Test
  void connect_releasesClientWhenExecutorFactoryFails() {
    KinesisPublisherConfig config =
        new KinesisPublisherConfig(
            Region.US_EAST_1,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")));
    KinesisPublisher p =
        new KinesisPublisher(
            config,
            c -> mockClient,
            c -> {
              throw new RuntimeException("exec boom");
            });
    assertThrows(RuntimeException.class, p::connect);
    verify(mockClient).close();
  }

  @Test
  void operationsBeforeConnect_throw() {
    Message m = new Message("id", "stream-a", new byte[0], null);
    assertThrows(IllegalStateException.class, () -> publisher.publish(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishSync(m));
    assertThrows(IllegalStateException.class, () -> publisher.publishBatch(List.of(m)));
    assertThrows(IllegalStateException.class, publisher::flush);
  }

  @Test
  void publish_callsPutRecordWithPartitionKeyAttribute() throws Exception {
    publisher.connect();
    when(mockClient.putRecord(any(PutRecordRequest.class)))
        .thenReturn(
            PutRecordResponse.builder().shardId("shard-1").sequenceNumber("12345").build());

    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put(KinesisConsumer.ATTR_PARTITION_KEY, "user-42");
    Message m = new Message("mid", "stream-a", new byte[] {1, 2, 3}, attrs);
    final CompletableFuture<PublishResult> future = publisher.publish(m);

    ArgumentCaptor<PutRecordRequest> captor = ArgumentCaptor.forClass(PutRecordRequest.class);
    verify(mockClient).putRecord(captor.capture());
    PutRecordRequest req = captor.getValue();
    assertEquals("stream-a", req.streamName());
    assertEquals("user-42", req.partitionKey());
    assertArrayEquals(new byte[] {1, 2, 3}, req.data().asByteArray());

    PublishResult result = future.get();
    assertEquals("shard-1", result.getShardId());
    assertEquals("12345", result.getSequenceNumber());
    assertEquals("shard-1:12345", result.getMessageId());
  }

  @Test
  void publish_fallsBackToMessageIdWhenNoPartitionKey() throws Exception {
    publisher.connect();
    when(mockClient.putRecord(any(PutRecordRequest.class)))
        .thenReturn(PutRecordResponse.builder().shardId("s").sequenceNumber("1").build());

    Message m = new Message("mid-fallback", "stream-a", new byte[0], null);
    publisher.publish(m).get();

    ArgumentCaptor<PutRecordRequest> captor = ArgumentCaptor.forClass(PutRecordRequest.class);
    verify(mockClient).putRecord(captor.capture());
    assertEquals("mid-fallback", captor.getValue().partitionKey());
  }

  @Test
  void publish_completesExceptionallyOnSdkException() {
    publisher.connect();
    KinesisException error = (KinesisException) KinesisException.builder().message("bad").build();
    when(mockClient.putRecord(any(PutRecordRequest.class))).thenThrow(error);
    Message m = new Message("a", "stream-a", new byte[0], null);
    CompletableFuture<PublishResult> cf = publisher.publish(m);
    ExecutionException ee = assertThrows(ExecutionException.class, cf::get);
    assertSame(error, ee.getCause());
  }

  @Test
  void publishSync_unwrapsRuntimeException() {
    publisher.connect();
    KinesisException error = (KinesisException) KinesisException.builder().message("bad").build();
    when(mockClient.putRecord(any(PutRecordRequest.class))).thenThrow(error);
    Message m = new Message("a", "stream-a", new byte[0], null);
    KinesisException thrown = assertThrows(KinesisException.class, () -> publisher.publishSync(m));
    assertSame(error, thrown);
  }

  @Test
  void publishBatch_emptyReturnsEmptyImmediately() throws Exception {
    publisher.connect();
    assertTrue(publisher.publishBatch(List.of()).get().isEmpty());
    verify(mockClient, never()).putRecords(any(PutRecordsRequest.class));
  }

  @Test
  void publishBatch_groupsByStreamAndChunksAt500() throws Exception {
    publisher.connect();
    when(mockClient.putRecords(any(PutRecordsRequest.class)))
        .thenAnswer(
            invocation -> {
              PutRecordsRequest req = invocation.getArgument(0);
              List<PutRecordsResultEntry> results = new ArrayList<>();
              for (int i = 0; i < req.records().size(); i++) {
                results.add(
                    PutRecordsResultEntry.builder()
                        .shardId("shard-1")
                        .sequenceNumber("seq-" + i)
                        .build());
              }
              return PutRecordsResponse.builder().records(results).build();
            });

    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < 700; i++) {
      messages.add(new Message("a-" + i, "stream-a", new byte[] {(byte) i}, null));
    }
    for (int i = 0; i < 500; i++) {
      messages.add(new Message("b-" + i, "stream-b", new byte[] {(byte) i}, null));
    }

    publisher.publishBatch(messages).get();

    ArgumentCaptor<PutRecordsRequest> captor = ArgumentCaptor.forClass(PutRecordsRequest.class);
    verify(mockClient, times(3)).putRecords(captor.capture());
    List<PutRecordsRequest> calls = captor.getAllValues();

    int chunksA = 0;
    int chunksB = 0;
    int totalA = 0;
    int totalB = 0;
    for (PutRecordsRequest call : calls) {
      assertTrue(call.records().size() <= 500);
      if ("stream-a".equals(call.streamName())) {
        chunksA++;
        totalA += call.records().size();
      } else if ("stream-b".equals(call.streamName())) {
        chunksB++;
        totalB += call.records().size();
      }
    }
    assertEquals(2, chunksA);
    assertEquals(1, chunksB);
    assertEquals(700, totalA);
    assertEquals(500, totalB);
  }

  @Test
  void publishBatch_perRecordPartialFailure() {
    publisher.connect();
    when(mockClient.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            PutRecordsResponse.builder()
                .records(
                    PutRecordsResultEntry.builder()
                        .shardId("s1")
                        .sequenceNumber("1")
                        .build(),
                    PutRecordsResultEntry.builder()
                        .errorCode("ProvisionedThroughputExceededException")
                        .errorMessage("rate limited")
                        .build(),
                    PutRecordsResultEntry.builder()
                        .shardId("s1")
                        .sequenceNumber("2")
                        .build())
                .build());

    Message m1 = new Message("a", "stream-a", new byte[] {1}, null);
    Message m2 = new Message("b", "stream-a", new byte[] {2}, null);
    Message m3 = new Message("c", "stream-a", new byte[] {3}, null);

    CompletableFuture<List<PublishResult>> batch = publisher.publishBatch(List.of(m1, m2, m3));
    ExecutionException ee = assertThrows(ExecutionException.class, batch::get);
    PublishBatchException pbe = (PublishBatchException) ee.getCause();
    assertEquals(2, pbe.getSucceeded().size());
    assertEquals(1, pbe.getFailures().size());
    Throwable failure = pbe.getFailures().get(1);
    assertTrue(failure.getMessage().contains("ProvisionedThroughputExceededException"));
    assertTrue(failure.getMessage().contains("rate limited"));
  }

  @Test
  void close_closesClientAndIsIdempotent() {
    publisher.connect();
    publisher.close();
    publisher.close();
    verify(mockClient, times(1)).close();
  }

  /** Synchronous executor: tasks run on the calling thread, mirroring directExecutor. */
  private static ExecutorService directExecutor() {
    return new AbstractExecutorService() {
      private volatile boolean shutdown;

      @Override
      public void shutdown() {
        shutdown = true;
      }

      @Override
      public List<Runnable> shutdownNow() {
        shutdown = true;
        return List.of();
      }

      @Override
      public boolean isShutdown() {
        return shutdown;
      }

      @Override
      public boolean isTerminated() {
        return shutdown;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
      }

      @Override
      public void execute(Runnable command) {
        command.run();
      }
    };
  }
}
