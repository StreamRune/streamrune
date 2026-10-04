package org.streamrune.micronaut;

import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.core.beans.exceptions.IntrospectionException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.executable.ExecutableValidator;
import jakarta.validation.metadata.BeanDescriptor;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Validator} decorator that treats Micronaut's "no bean introspection" failure as "no
 * constraints" instead of an error.
 *
 * <p>Micronaut Validation is compile-time: its validator can only validate classes annotated with
 * {@code @Introspected} (or registered via {@code @Introspected(classes = ...)}) and throws a
 * {@code ValidationException} for everything else. Command classes are framework-agnostic domain
 * types and frequently lack that annotation — without this guard every command dispatch would fail.
 * When the delegate is Micronaut's validator, non-introspected types are skipped and logged once
 * per type so the gap is visible. Reflection-based validators (e.g., Hibernate Validator) are
 * delegated to unconditionally.
 */
final class IntrospectionSafeValidator implements Validator {

  private static final Logger logger = LoggerFactory.getLogger(IntrospectionSafeValidator.class);

  private final Validator delegate;
  private final boolean requiresIntrospection;
  private final Set<Class<?>> warnedTypes = ConcurrentHashMap.newKeySet();

  IntrospectionSafeValidator(Validator delegate) {
    this.delegate = delegate;
    this.requiresIntrospection = delegate instanceof io.micronaut.validation.validator.Validator;
  }

  @Override
  public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups) {
    if (object != null && skipNonIntrospected(object.getClass())) {
      return Set.of();
    }
    try {
      return delegate.validate(object, groups);
    } catch (IntrospectionException _) {
      warnOnce(object.getClass());
      return Set.of();
    }
  }

  @Override
  public <T> Set<ConstraintViolation<T>> validateProperty(
      T object, String propertyName, Class<?>... groups) {
    if (object != null && skipNonIntrospected(object.getClass())) {
      return Set.of();
    }
    try {
      return delegate.validateProperty(object, propertyName, groups);
    } catch (IntrospectionException _) {
      warnOnce(object.getClass());
      return Set.of();
    }
  }

  @Override
  public <T> Set<ConstraintViolation<T>> validateValue(
      Class<T> beanType, String propertyName, Object value, Class<?>... groups) {
    if (skipNonIntrospected(beanType)) {
      return Set.of();
    }
    try {
      return delegate.validateValue(beanType, propertyName, value, groups);
    } catch (IntrospectionException _) {
      warnOnce(beanType);
      return Set.of();
    }
  }

  @Override
  public BeanDescriptor getConstraintsForClass(Class<?> clazz) {
    return delegate.getConstraintsForClass(clazz);
  }

  @Override
  public <T> T unwrap(Class<T> type) {
    return delegate.unwrap(type);
  }

  @Override
  public ExecutableValidator forExecutables() {
    return delegate.forExecutables();
  }

  private boolean skipNonIntrospected(Class<?> type) {
    if (!requiresIntrospection) {
      return false;
    }
    if (BeanIntrospector.SHARED.findIntrospection(type).isPresent()) {
      return false;
    }
    warnOnce(type);
    return true;
  }

  private void warnOnce(Class<?> type) {
    if (warnedTypes.add(type)) {
      logger.warn(
          "Skipping validation for {}: no Micronaut bean introspection. "
              + "Annotate the class with @Introspected (or register it via "
              + "@Introspected(classes = ...)) to enable constraint validation.",
          type.getName());
    }
  }
}
