package org.streamrune.micronaut;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.ReflectiveAccess;
import jakarta.inject.Singleton;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a StreamRune component for auto-registration in Micronaut.
 *
 * <p>Applied to Decider, Projection, and QueryHandler implementations. Micronaut's compile-time DI
 * picks up annotated classes via annotation processor.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @StreamRuneComponent
 * public class CartDecider implements Decider<CartCommand, CartState, CartEvent> { }
 * }</pre>
 */
@Singleton
@Introspected
@ReflectiveAccess
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StreamRuneComponent {}
