package org.streamrune.integration.tck;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Flattens an integration's configuration properties into the keys it binds and their defaults:
 * {@code Map<key, default>}, the default rendered by {@link DefaultRendering}. The doc-lint tests
 * of the three integrations compare the guides, the module READMEs and the configuration metadata
 * with this map, so the map has to be read from the properties exactly as the framework binds them:
 * each test builds its instance the way its framework does (Spring's {@code Binder} on an empty
 * source, SmallRye Config with no source, a started Micronaut context) and hands it here, so a
 * default set by a compact constructor or by an annotation on a nested type is the value the
 * framework produces, not a reading of annotations.
 *
 * <p>A record is walked through its components, an interface (a SmallRye {@code @ConfigMapping})
 * through its abstract methods without parameters. A component or method whose type is a record, or
 * an interface outside {@code java.*}, is a group: its own components become keys under its key.
 * Everything else is a key of its own. Where the key of a component comes from is the framework's
 * rule, given as a {@link KeyNaming}.
 */
public final class ConfigurationOracle {

  /** How a component or a method of a properties type becomes a key. */
  @FunctionalInterface
  public interface KeyNaming {

    /**
     * The full key of {@code element}, a record component or an interface method named {@code
     * javaName}, inside the group whose key is {@code parentKey}.
     */
    String key(String parentKey, AnnotatedElement element, String javaName);

    /** {@code parentKey}, a dot, and {@code javaName} in {@link #kebab kebab case}. */
    KeyNaming KEBAB = (parentKey, element, javaName) -> parentKey + "." + kebab(javaName);
  }

  private ConfigurationOracle() {}

  /** The keys and defaults of a record tree, named in {@link KeyNaming#KEBAB kebab case}. */
  public static Map<String, String> ofRecord(Object instance, String prefix) {
    return ofRecord(instance, prefix, KeyNaming.KEBAB);
  }

  /** The keys and defaults of a record tree, named by {@code naming}. */
  public static Map<String, String> ofRecord(Object instance, String prefix, KeyNaming naming) {
    if (!instance.getClass().isRecord()) {
      throw new IllegalArgumentException(instance.getClass().getName() + " is not a record");
    }
    Map<String, String> keys = new TreeMap<>();
    walk(instance, instance.getClass(), prefix, naming, keys);
    return keys;
  }

  /**
   * The keys and defaults of {@code mapping}, an instance of the {@code @ConfigMapping} interface
   * {@code type} built by SmallRye Config, named by {@code naming}.
   */
  public static Map<String, String> ofConfigMapping(
      Object mapping, Class<?> type, String prefix, KeyNaming naming) {
    if (!type.isInterface() || !type.isInstance(mapping)) {
      throw new IllegalArgumentException(
          "expected an instance of the interface " + type.getName() + ", got " + mapping);
    }
    Map<String, String> keys = new TreeMap<>();
    walk(mapping, type, prefix, naming, keys);
    return keys;
  }

  /**
   * Spring's canonical dashed form of a Java name, which is also SmallRye's kebab case: a dash
   * before every upper-case letter that starts a word, everything in lower case ({@code
   * snapshotEveryNEvents} becomes {@code snapshot-every-n-events}).
   */
  public static String kebab(String javaName) {
    return javaName
        .replaceAll("([a-z0-9])([A-Z])", "$1-$2")
        .replaceAll("([A-Z])([A-Z][a-z])", "$1-$2")
        .toLowerCase(Locale.ROOT);
  }

  private static void walk(
      Object group, Class<?> type, String key, KeyNaming naming, Map<String, String> keys) {
    if (type.isRecord()) {
      for (RecordComponent component : type.getRecordComponents()) {
        Object value = group == null ? null : read(component.getAccessor(), group);
        visit(
            value,
            component.getType(),
            component.getGenericType(),
            naming.key(key, component, component.getName()),
            naming,
            keys);
      }
    } else {
      Method[] methods =
          Arrays.stream(type.getMethods())
              .filter(m -> Modifier.isAbstract(m.getModifiers()) && m.getParameterCount() == 0)
              .sorted(Comparator.comparing(Method::getName))
              .toArray(Method[]::new);
      for (Method method : methods) {
        Object value = group == null ? null : read(method, group);
        visit(
            value,
            method.getReturnType(),
            method.getGenericReturnType(),
            naming.key(key, method, method.getName()),
            naming,
            keys);
      }
    }
  }

  private static void visit(
      Object value,
      Class<?> type,
      Type genericType,
      String key,
      KeyNaming naming,
      Map<String, String> keys) {
    Class<?> groupType = groupType(type, genericType);
    if (groupType != null) {
      Object group = value instanceof Optional<?> optional ? optional.orElse(null) : value;
      walk(group, groupType, key, naming, keys);
      return;
    }
    String previous = keys.put(key, DefaultRendering.render(value));
    if (previous != null) {
      throw new IllegalStateException("two properties bind the key " + key);
    }
  }

  /**
   * The group type a component or method of {@code type} opens, or {@code null} for a key: a
   * record, an interface outside {@code java.*}, or such a type inside an {@link Optional}.
   */
  private static Class<?> groupType(Class<?> type, Type genericType) {
    if (type == Optional.class
        && genericType instanceof ParameterizedType parameterized
        && parameterized.getActualTypeArguments()[0] instanceof Class<?> element) {
      return groupType(element, element);
    }
    boolean group = type.isRecord() || (type.isInterface() && !type.getName().startsWith("java."));
    return group ? type : null;
  }

  private static Object read(Method accessor, Object target) {
    try {
      accessor.setAccessible(true);
      return accessor.invoke(target);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("cannot read " + accessor, e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException("reading " + accessor + " failed", e.getCause());
    }
  }
}
