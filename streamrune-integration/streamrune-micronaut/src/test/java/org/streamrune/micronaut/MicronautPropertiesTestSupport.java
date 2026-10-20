package org.streamrune.micronaut;

/** Builds {@link StreamRuneMicronautProperties} variants without spelling out every component. */
final class MicronautPropertiesTestSupport {

  private MicronautPropertiesTestSupport() {}

  /** {@code base} with one record component replaced — the canonical constructor re-validates. */
  static StreamRuneMicronautProperties with(
      StreamRuneMicronautProperties base, String component, Object value) throws Exception {
    var components = StreamRuneMicronautProperties.class.getRecordComponents();
    Object[] args = new Object[components.length];
    Class<?>[] types = new Class<?>[components.length];
    boolean found = false;
    for (int i = 0; i < components.length; i++) {
      types[i] = components[i].getType();
      if (components[i].getName().equals(component)) {
        args[i] = value;
        found = true;
      } else {
        args[i] = components[i].getAccessor().invoke(base);
      }
    }
    if (!found) {
      throw new IllegalArgumentException("no record component " + component);
    }
    return StreamRuneMicronautProperties.class.getDeclaredConstructor(types).newInstance(args);
  }
}
