package org.streamrune.core.audit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a query class for audit logging. When dispatched through an {@link
 * org.streamrune.core.QueryBus} wrapped with auditing support, the query execution is recorded in
 * the audit trail.
 *
 * <p>Only queries annotated with {@code @Auditable} are logged; all others pass through silently.
 *
 * <p><b>Interaction with {@link org.streamrune.core.Cacheable @Cacheable}:</b> auditing is a query
 * bus decorator and only observes dispatches that flow through it. For a query that is both
 * {@code @Auditable} and {@code @Cacheable}, the nesting order of the auditing and caching
 * decorators determines whether cache hits are audited: with auditing outermost (auditing wraps
 * caching) every dispatch is recorded, including cache hits; with caching outermost, cache hits
 * return from the cache before reaching the auditing decorator and are never recorded. The provided
 * Spring, Quarkus and Micronaut integrations wire auditing outermost, so cache hits are audited.
 * Hand-wired buses must preserve that order to keep a complete audit trail.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Auditable {}
