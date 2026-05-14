package com.unifieddataprocessing.pubsub.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory {@link SchemaRegistry}.
 *
 * <p>Per-subject version history is stored as an {@link ArrayList} guarded by the subject's own
 * list monitor. {@link #register(String, String, String)} routes through {@link
 * ConcurrentHashMap#compute(Object, java.util.function.BiFunction)} so version allocation is
 * atomic even across concurrent registrations on the same subject, while reads on other subjects
 * never block the writer. Snapshots returned to callers are defensive copies so external mutation
 * of a returned list / set cannot corrupt registry state.
 */
public final class InMemorySchemaRegistry implements SchemaRegistry {

  private final ConcurrentHashMap<String, List<Schema>> versionsBySubject =
      new ConcurrentHashMap<>();

  @Override
  public Schema register(String subject, String type, String definition) {
    Objects.requireNonNull(subject, "subject");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(definition, "definition");
    Schema[] result = new Schema[1];
    versionsBySubject.compute(
        subject,
        (s, history) -> {
          List<Schema> updated = history == null ? new ArrayList<>() : history;
          int nextVersion = updated.size() + 1;
          Schema created = new Schema(s, nextVersion, type, definition);
          updated.add(created);
          result[0] = created;
          return updated;
        });
    return result[0];
  }

  @Override
  public Optional<Schema> latest(String subject) {
    Objects.requireNonNull(subject, "subject");
    List<Schema> history = versionsBySubject.get(subject);
    if (history == null) {
      return Optional.empty();
    }
    synchronized (history) {
      if (history.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(history.get(history.size() - 1));
    }
  }

  @Override
  public Schema get(String subject, int version) {
    Objects.requireNonNull(subject, "subject");
    if (version < 1) {
      throw new IllegalArgumentException("version must be >= 1, got " + version);
    }
    List<Schema> history = versionsBySubject.get(subject);
    if (history == null) {
      throw new SubjectNotFoundException(subject);
    }
    synchronized (history) {
      if (version > history.size()) {
        throw new VersionNotFoundException(subject, version);
      }
      return history.get(version - 1);
    }
  }

  @Override
  public List<Integer> versions(String subject) {
    Objects.requireNonNull(subject, "subject");
    List<Schema> history = versionsBySubject.get(subject);
    if (history == null) {
      return List.of();
    }
    synchronized (history) {
      List<Integer> snapshot = new ArrayList<>(history.size());
      for (Schema s : history) {
        snapshot.add(s.version());
      }
      return Collections.unmodifiableList(snapshot);
    }
  }

  @Override
  public Set<String> subjects() {
    return Collections.unmodifiableSet(new HashSet<>(versionsBySubject.keySet()));
  }

  @Override
  public boolean deleteSubject(String subject) {
    Objects.requireNonNull(subject, "subject");
    return versionsBySubject.remove(subject) != null;
  }
}
