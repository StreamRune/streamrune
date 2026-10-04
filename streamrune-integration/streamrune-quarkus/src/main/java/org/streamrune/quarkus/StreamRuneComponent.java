package org.streamrune.quarkus;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Stereotype;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a StreamRune component for auto-registration in Quarkus.
 *
 * <p>Applied to Decider, Projection, and QueryHandler implementations. CDI auto-discovers annotated
 * classes at build time.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @StreamRuneComponent
 * public class CartDecider implements Decider<CartCommand, CartState, CartEvent> { }
 * }</pre>
 */
@Stereotype
@ApplicationScoped
@RegisterForReflection
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StreamRuneComponent {}
