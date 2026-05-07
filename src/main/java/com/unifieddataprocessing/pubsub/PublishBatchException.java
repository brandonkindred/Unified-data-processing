package com.unifieddataprocessing.pubsub;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Thrown when {@link PubSubPublisher#publishBatch(List)} completes with at least one per-message
 * failure. {@link #getSucceeded()} contains the results for the inputs that did publish; {@link
 * #getFailures()} maps each failed input index to its cause. The first failure (lowest index) is
 * also installed as the {@linkplain #getCause() cause}.
 */
public final class PublishBatchException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final transient List<PublishResult> succeeded;
  private final transient Map<Integer, Throwable> failures;

  /**
   * Creates a batch exception describing partial success.
   *
   * @param succeeded results for the inputs that published successfully (any order)
   * @param failures map of input index to failure cause; must be non-empty
   */
  public PublishBatchException(List<PublishResult> succeeded, Map<Integer, Throwable> failures) {
    super(buildMessage(failures), firstFailure(failures));
    this.succeeded =
        succeeded == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new java.util.ArrayList<>(succeeded));
    this.failures =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(Objects.requireNonNull(failures, "failures")));
    Throwable first = firstFailure(failures);
    for (Map.Entry<Integer, Throwable> e : this.failures.entrySet()) {
      if (e.getValue() != first && e.getValue() != null) {
        addSuppressed(e.getValue());
      }
    }
  }

  public List<PublishResult> getSucceeded() {
    return succeeded;
  }

  public Map<Integer, Throwable> getFailures() {
    return failures;
  }

  private static Throwable firstFailure(Map<Integer, Throwable> failures) {
    if (failures == null || failures.isEmpty()) {
      return null;
    }
    Integer firstKey = null;
    for (Integer key : failures.keySet()) {
      if (firstKey == null || key < firstKey) {
        firstKey = key;
      }
    }
    return failures.get(firstKey);
  }

  private static String buildMessage(Map<Integer, Throwable> failures) {
    int n = failures == null ? 0 : failures.size();
    return "publishBatch had " + n + " failure" + (n == 1 ? "" : "s");
  }
}
