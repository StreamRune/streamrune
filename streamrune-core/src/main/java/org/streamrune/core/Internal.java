package org.streamrune.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type or member that is public only because another StreamRune module uses it: the
 * runtime, the PostgreSQL store or one of the framework integrations. It is not part of the public
 * API.
 *
 * <p>An element marked {@code @Internal} is not covered by the compatibility policy of the README's
 * "API stability" section: it may change or disappear in any release, a patch release included.
 * Application code does not call it, implement it or bind it. The generated API documentation
 * leaves such an element out (it also carries the {@code @hidden} javadoc tag).
 *
 * <p>No component reads this annotation; it documents a promise and is kept in the class files so
 * that tools can see it.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
public @interface Internal {}
