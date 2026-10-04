package org.streamrune.test;

import java.lang.classfile.ClassFile;
import java.lang.classfile.attribute.PermittedSubclassesAttribute;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds, on the JVM, a sealed type in the shape a GraalVM native image reports for a sealed type
 * that was not registered for reflection: {@link Class#isSealed()} is {@code true} while {@link
 * Class#getPermittedSubclasses()} is an empty array. Measured on GraalVM CE 25.0.1: without
 * reflection metadata for the sealed type itself, the image answers exactly that (registering only
 * its subclasses does not help), and with {@code --exact-reachability-metadata} {@code
 * getPermittedSubclasses()} throws {@code MissingReflectionRegistrationError} instead.
 *
 * <p>The JVM gives the same answer for a sealed interface whose only permitted subclass cannot be
 * loaded, which is how this fixture produces it: the generated interface names a permitted subclass
 * that does not exist. Use it to pin that a walk over sealed hierarchies refuses such a type rather
 * than treating it as a leaf.
 */
public final class UnregisteredSealedTypes {

  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private UnregisteredSealedTypes() {}

  /**
   * Defines a new public sealed interface in the package of {@code lookup}'s class (each call a
   * distinct class) whose permitted subclasses cannot be read.
   *
   * @param lookup a full-privilege lookup of a class in the package to define the interface in,
   *     typically {@code MethodHandles.lookup()} of the calling test
   * @return the defined interface; {@code isSealed()} is {@code true} and {@code
   *     getPermittedSubclasses()} is empty
   */
  public static Class<?> sealedInterface(MethodHandles.Lookup lookup) {
    String name =
        lookup.lookupClass().getPackageName()
            + ".UnregisteredSealedRoot"
            + SEQUENCE.incrementAndGet();
    byte[] bytes =
        ClassFile.of()
            .build(
                ClassDesc.of(name),
                cb ->
                    cb.withFlags(
                            ClassFile.ACC_PUBLIC | ClassFile.ACC_INTERFACE | ClassFile.ACC_ABSTRACT)
                        .withSuperclass(ConstantDescs.CD_Object)
                        .with(
                            PermittedSubclassesAttribute.ofSymbols(
                                ClassDesc.of(name + "$NeverLoaded"))));
    try {
      return lookup.defineClass(bytes);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          "lookup cannot define a class in " + lookup.lookupClass().getPackageName(), e);
    }
  }
}
