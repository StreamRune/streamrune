package org.streamrune.quarkus.graal;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.HashMap;
import org.flywaydb.core.internal.scanner.LocationScannerCache;
import org.flywaydb.core.internal.scanner.ResourceNameCache;
import org.flywaydb.core.internal.scanner.classpath.ClassPathLocationScanner;
import org.flywaydb.core.internal.scanner.classpath.FileSystemClassPathLocationScanner;

/**
 * Native-image substitution for Flyway's class-path scanner: it picks a location scanner for the
 * {@code file} and archive protocols as Flyway does, and none for JBoss VFS or OSGi.
 *
 * <p><b>Why.</b> Since Flyway 13 a {@code Flyway} instance reaches this scanner as soon as it is
 * built (it looks for callbacks on the class path), and the scanner's {@code createLocationScanner}
 * names Flyway's JBoss VFS and OSGi scanners, which need {@code jboss-vfs} and {@code
 * org.osgi.core} — optional Flyway dependencies an application does not have. Quarkus links every
 * class at image build time, so its native build failed on them ({@code Discovered unresolved type
 * during parsing: org.jboss.vfs.VirtualFileFilter}, then {@code org.osgi.framework.FrameworkUtil}).
 * A native image has neither a JBoss VFS nor an OSGi class path: its class loader serves resources
 * under the {@code resource} protocol. So in an image those two branches can never be taken, and
 * this substitution leaves them out; the other branches are Flyway's own.
 *
 * <p>StreamRune does not depend on this scanner: it hands Flyway its migration scripts by name (see
 * {@code ShippedMigrationSeries} in {@code streamrune-postgres}). Only a native image applies this
 * class; on the JVM Flyway runs unchanged.
 */
@TargetClass(className = "org.flywaydb.core.internal.scanner.classpath.ClassPathScanner")
final class Target_ClassPathScanner {

  @Alias private LocationScannerCache locationScannerCache;

  @Alias private ResourceNameCache resourceNameCache;

  private Target_ClassPathScanner() {}

  @Substitute
  private ClassPathLocationScanner createLocationScanner(String protocol) {
    if (locationScannerCache.containsKey(protocol)) {
      return locationScannerCache.get(protocol);
    }
    ClassPathLocationScanner scanner;
    if ("file".equals(protocol)) {
      scanner = new FileSystemClassPathLocationScanner();
    } else if ("jar".equals(protocol)
        || "war".equals(protocol) // Tomcat
        || "zip".equals(protocol) // WebLogic
        || "wsjar".equals(protocol)) { // WebSphere
      String separator = "war".equals(protocol) ? "*/" : "!/";
      scanner =
          (ClassPathLocationScanner) (Object) new Target_JarFileClassPathLocationScanner(separator);
    } else {
      return null; // vfs (JBoss), bundle / bundleresource (OSGi) and any other protocol
    }
    locationScannerCache.put(protocol, scanner);
    resourceNameCache.put(scanner, new HashMap<>());
    return scanner;
  }
}

/** Flyway's jar scanner, whose constructor is package-private. */
@TargetClass(
    className = "org.flywaydb.core.internal.scanner.classpath.JarFileClassPathLocationScanner")
final class Target_JarFileClassPathLocationScanner {

  @Alias
  Target_JarFileClassPathLocationScanner(String separator) {}
}
