package com.unifieddataprocessing.pubsub.bridge;

import java.time.Duration;

/**
 * Indirection over {@link Thread#sleep(long)} so the bridge's backoff logic can be tested
 * deterministically without real time passing.
 */
interface Sleeper {

  void sleep(Duration d) throws InterruptedException;

  static Sleeper real() {
    return d -> Thread.sleep(d.toMillis());
  }
}
