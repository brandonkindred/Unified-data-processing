package com.unifieddataprocessing.pubsub.schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Append-only registry of {@link Schema} versions keyed by subject.
 *
 * <p>For the data bridge a subject is the target Kafka topic name. {@link #register(String, String,
 * String)} allocates a new monotonically-increasing version starting at {@code 1} and returns the
 * resulting {@link Schema}. Existing versions are immutable: once registered, a {@code (subject,
 * version)} pair always resolves to the same body. Implementations must be safe for concurrent use
 * by both bridge worker threads (read path) and a control-plane caller (write path).
 *
 * <p>Lookups by {@code (subject, version)} that do not exist throw {@link
 * SubjectNotFoundException} or {@link VersionNotFoundException}; {@link #latest(String)} returns
 * {@link Optional#empty()} for unknown subjects so the bridge can treat "no schema configured" as a
 * silent pass-through rather than an error.
 */
public interface SchemaRegistry {

  /**
   * Registers a new version of {@code subject} carrying {@code type} and {@code definition}, and
   * returns the resulting {@link Schema}. The first call for a subject allocates version {@code 1};
   * subsequent calls allocate strictly increasing versions. This method is the only mutator on the
   * registry, so concurrent callers see a totally-ordered version history.
   */
  Schema register(String subject, String type, String definition);

  /**
   * Returns the highest-versioned {@link Schema} for {@code subject}, or {@link Optional#empty()}
   * if no version has been registered. The bridge uses this on the hot path to decide whether to
   * validate a message at all.
   */
  Optional<Schema> latest(String subject);

  /**
   * Returns the {@link Schema} for the exact {@code (subject, version)} pair. Throws {@link
   * SubjectNotFoundException} if the subject has no versions, or {@link VersionNotFoundException}
   * if the subject exists but the requested version does not.
   */
  Schema get(String subject, int version);

  /**
   * Returns the registered version numbers for {@code subject} in ascending order, or an empty
   * list if the subject has no versions.
   */
  List<Integer> versions(String subject);

  /** Returns every subject that has at least one registered version. */
  Set<String> subjects();

  /**
   * Removes every version of {@code subject}. Returns {@code true} if the subject existed and was
   * removed; {@code false} if no versions were registered.
   */
  boolean deleteSubject(String subject);
}
