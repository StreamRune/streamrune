package org.streamrune.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.stereotype.Component;

/**
 * Marks a class as a StreamRune component so it is registered as a Spring bean.
 *
 * <p>Meta-annotated with {@link Component}, so annotated classes are picked up by the application's
 * regular component scan — they must live inside a scanned package (typically under the
 * {@code @SpringBootApplication} class). This mirrors the Micronaut ({@code @Singleton}) and
 * Quarkus ({@code @ApplicationScoped} stereotype) variants of this annotation.
 *
 * <p>Applied to:
 *
 * <ul>
 *   <li>Projection implementations — beans implementing {@code Projection} and annotated with
 *       {@code @ProjectionConfig} are auto-registered with the projection runners
 *   <li>Decider implementations — registered as beans; wiring into the command bus still requires
 *       an explicit {@code DeciderRegistration} bean (the command type and ID extractor cannot be
 *       derived automatically)
 *   <li>QueryHandler implementations — registered as beans for explicit registration with a query
 *       bus
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * @StreamRuneComponent
 * @ProjectionConfig(name = "cart", deliveryMode = AT_LEAST_ONCE_IDEMPOTENT)
 * public class CartProjection implements Projection { }
 * }</pre>
 *
 * <p><b>Delivery mode:</b> {@code @ProjectionConfig.deliveryMode()} is required. A plain {@code
 * implements Projection} bean like the one above declares {@code AT_LEAST_ONCE_IDEMPOTENT} and is
 * handed no transaction-scoped repository, so its writes must be idempotent; for a read model that
 * must be atomic with its checkpoint, extend {@code org.streamrune.core.projection.BaseProjection}
 * over the {@code JdbcProjectionRepository} bean and declare {@code TRANSACTIONAL_LOCAL}.
 */
@Component
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StreamRuneComponent {}
