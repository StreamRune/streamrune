package org.streamrune.core.gdpr;

import com.fasterxml.jackson.databind.JsonNode;
import org.streamrune.core.types.SubjectId;

/**
 * Application-provided data source contributing one named section to a subject's GDPR data export.
 * Implementations are typically thin adapters over read models, projections, or external systems
 * holding data tied to a data subject.
 *
 * <p>Each collector is stateless and may be invoked concurrently for different subjects. The export
 * service invokes collectors sequentially for any given subject.
 */
public interface SubjectDataCollector {

  /**
   * Stable section name used as the JSON key under which this collector's output is placed in the
   * final {@link SubjectExport}. Must be non-blank, unique within the registered set of collectors,
   * and stable across releases (consumers may parse it).
   *
   * <p>The export service reads this once, when it is built, and keys this collector's section by
   * it in every export; it never calls it during an export. Two collectors under one name are
   * refused there: the service's builder throws {@link IllegalArgumentException} naming them, as
   * one section would otherwise replace the other. A {@code name()} that throws an exception, or
   * returns {@code null} or a blank string, does not stop the build: the collector is named by its
   * class name instead, with a WARN naming the class, so two such instances of one class are
   * refused as a shared name too. An {@link Error} from it propagates from the builder. Return a
   * stable, literal name: a class name changes when the class is renamed or moved, and the keys of
   * an export are what a consumer parses.
   *
   * @return the section name
   */
  String name();

  /**
   * Collects all data tied to the given subject from this collector's source.
   *
   * @param subjectId the data subject identifier; never null
   * @return a {@link JsonNode} representing the section payload, or {@code null} if no data exists
   *     for this subject (the section will be omitted from the export)
   */
  JsonNode collect(SubjectId subjectId);
}
