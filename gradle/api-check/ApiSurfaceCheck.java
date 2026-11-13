import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.classfile.AccessFlags;
import java.lang.classfile.Annotation;
import java.lang.classfile.AttributedElement;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.ClassSignature;
import java.lang.classfile.FieldModel;
import java.lang.classfile.MethodModel;
import java.lang.classfile.MethodSignature;
import java.lang.classfile.Signature;
import java.lang.classfile.attribute.ConstantValueAttribute;
import java.lang.classfile.attribute.ExceptionsAttribute;
import java.lang.classfile.attribute.InnerClassInfo;
import java.lang.classfile.attribute.InnerClassesAttribute;
import java.lang.classfile.attribute.ModuleAttribute;
import java.lang.classfile.attribute.ModuleExportInfo;
import java.lang.classfile.attribute.PermittedSubclassesAttribute;
import java.lang.classfile.attribute.RuntimeInvisibleAnnotationsAttribute;
import java.lang.classfile.attribute.RuntimeVisibleAnnotationsAttribute;
import java.lang.classfile.attribute.SignatureAttribute;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.StringEntry;
import java.lang.reflect.AccessFlag;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * The public API of one published module, written to and compared with the module's checked-in
 * {@code api/<module>.api} file, plus the rules of the {@code @Internal} marker. The root build
 * runs it on the module's compiled classes through two tasks of every published module:
 * {@code verifyApiSurface} (part of {@code check}) and {@code apiDump}.
 *
 * <p><b>What the file lists.</b> Every public type of the module that application code can reach
 * (public, or protected inside a public non-final type; in an exported package when the module
 * has a {@code module-info}; not marked {@code @Internal} and not nested in such a type), with its
 * kind, modifiers, type parameters, supertypes and permitted subtypes, and its public members (and
 * protected members of a non-final type) that are not marked {@code @Internal}: constructors,
 * methods with their generic signatures and thrown exceptions, fields with their constant values.
 * A change to any of it changes the file, so a change to the public API is made on purpose and is
 * visible in review: {@code verifyApiSurface} fails until {@code ./gradlew apiDump} rewrites the
 * file. Its message lists the lines that were removed or changed apart from the lines that were
 * added; a removed or changed line breaks code written against the API.
 *
 * <p><b>The {@code @Internal} rules</b> (both tasks fail on a violation):
 *
 * <ul>
 *   <li>no listed signature names a StreamRune type that is not public API: a type marked {@code
 *       @Internal} or nested in one, a type in a package its module does not export, or a type
 *       that is not public;
 *   <li>every declaration marked {@code @Internal} in the module's sources carries the {@code
 *       @hidden} javadoc tag, so the generated API documentation leaves it out.
 * </ul>
 *
 * <p>Arguments: {@code --mode check|dump}, {@code --module <gradle path>}, {@code --baseline
 * <file>}, {@code --report <file>}, and one or more {@code --classes <dir>}, {@code --classpath
 * <dir or jar>} and {@code --sources <dir>}. It needs nothing but the JDK and runs with the source
 * launcher ({@code java ApiSurfaceCheck.java ...}).
 */
public final class ApiSurfaceCheck {

  private static final String INTERNAL = "Lorg/streamrune/core/Internal;";
  private static final String OWN_PREFIX = "org/streamrune/";

  private final Map<String, Info> own = new TreeMap<>();
  private final List<Root> classpath = new ArrayList<>();
  private final Map<String, Optional<Info>> resolved = new HashMap<>();
  private final Set<String> unresolved = new TreeSet<>();

  public static void main(String[] args) throws IOException {
    Map<String, List<String>> options = parse(args);
    String mode = one(options, "--mode");
    String module = one(options, "--module");
    Path baseline = Path.of(one(options, "--baseline"));
    Path report = Path.of(one(options, "--report"));
    if (!mode.equals("check") && !mode.equals("dump")) {
      throw new IllegalArgumentException("--mode must be check or dump, not " + mode);
    }
    ApiSurfaceCheck check = new ApiSurfaceCheck();
    for (String dir : options.getOrDefault("--classes", List.of())) {
      check.readOwn(Path.of(dir));
    }
    for (String entry : options.getOrDefault("--classpath", List.of())) {
      Path path = Path.of(entry);
      if (Files.exists(path)) {
        check.classpath.add(new Root(path));
      }
    }
    List<String> api = check.dump();
    List<String> violations = new ArrayList<>(check.exposures());
    for (String dir : options.getOrDefault("--sources", List.of())) {
      violations.addAll(hiddenViolations(Path.of(dir)));
    }
    String header =
        "# The public API of "
            + module
            + ". verifyApiSurface (part of check) compares the build with this file;\n"
            + "# after a deliberate change to the public API, ./gradlew apiDump rewrites it.\n";
    String text = header + String.join("\n", api) + (api.isEmpty() ? "" : "\n");
    boolean failed = false;
    if (mode.equals("dump")) {
      Files.createDirectories(baseline.toAbsolutePath().getParent());
      Files.writeString(baseline, text, StandardCharsets.UTF_8);
      System.out.println(module + ": wrote " + baseline);
    } else {
      String expected =
          Files.exists(baseline) ? Files.readString(baseline, StandardCharsets.UTF_8) : null;
      if (expected == null) {
        System.err.println(
            module
                + " has no API baseline at "
                + baseline
                + ". Run ./gradlew apiDump and commit the file.");
        failed = true;
      } else if (!expected.equals(text)) {
        System.err.println(difference(module, baseline, expected, text));
        failed = true;
      }
    }
    if (!violations.isEmpty()) {
      System.err.println(
          module
              + " breaks the rules of the @Internal marker (README, \"API stability\"):\n  "
              + String.join("\n  ", violations));
      failed = true;
    }
    if (!check.unresolved.isEmpty()) {
      System.out.println(
          module
              + ": not found on the compile class path, not checked: "
              + String.join(", ", check.unresolved));
    }
    Files.createDirectories(report.toAbsolutePath().getParent());
    Files.writeString(
        report,
        module
            + "\nmode: "
            + mode
            + "\ntypes: "
            + api.stream().filter(l -> !l.isEmpty() && !l.startsWith(" ")).count()
            + "\nresult: "
            + (failed ? "FAILED" : "ok")
            + "\n",
        StandardCharsets.UTF_8);
    if (failed) {
      System.exit(1);
    }
  }

  // ── Class files ─────────────────────────────────────────────────────────────────────────────

  /** One class, with the access flags and the outer class its InnerClasses entry gives. */
  private record Info(
      String name, ClassModel model, int access, String outer, boolean local, boolean exported) {

    boolean has(AccessFlag flag) {
      return (access & flag.mask()) != 0;
    }

    boolean marked() {
      return isMarked(model);
    }
  }

  /** A class path entry: a directory or a jar, with the packages its module exports. */
  private static final class Root {
    private final Path path;
    private ZipFile zip;
    private Set<String> exports;
    private boolean exportsRead;

    Root(Path path) {
      this.path = path;
    }

    byte[] read(String entry) {
      try {
        if (Files.isDirectory(path)) {
          Path file = path.resolve(entry);
          return Files.isRegularFile(file) ? Files.readAllBytes(file) : null;
        }
        if (zip == null) {
          zip = new ZipFile(path.toFile());
        }
        ZipEntry e = zip.getEntry(entry);
        return e == null ? null : zip.getInputStream(e).readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /** The packages exported to everyone, or null when the root has no module descriptor. */
    Set<String> exports() {
      if (!exportsRead) {
        exportsRead = true;
        byte[] bytes = read("module-info.class");
        exports = bytes == null ? null : exportsOf(ClassFile.of().parse(bytes));
      }
      return exports;
    }
  }

  private static Set<String> exportsOf(ClassModel moduleInfo) {
    Set<String> packages = new TreeSet<>();
    moduleInfo
        .findAttribute(Attributes.module())
        .map(ModuleAttribute::exports)
        .orElse(List.of())
        .stream()
        .filter(e -> e.exportsTo().isEmpty())
        .map(ModuleExportInfo::exportedPackage)
        .forEach(p -> packages.add(p.name().stringValue()));
    return packages;
  }

  private void readOwn(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) {
      return;
    }
    Root root = new Root(dir);
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".class")).toList()) {
        String entry = dir.relativize(file).toString().replace('\\', '/');
        if (entry.endsWith("module-info.class") || entry.endsWith("package-info.class")) {
          continue;
        }
        Info info = info(ClassFile.of().parse(Files.readAllBytes(file)), root);
        own.put(info.name(), info);
      }
    }
  }

  private static Info info(ClassModel model, Root root) {
    String name = model.thisClass().asInternalName();
    int access = model.flags().flagsMask();
    String outer = null;
    boolean local = false;
    Optional<InnerClassesAttribute> inner = model.findAttribute(Attributes.innerClasses());
    if (inner.isPresent()) {
      for (InnerClassInfo entry : inner.get().classes()) {
        if (entry.innerClass().asInternalName().equals(name)) {
          access = entry.flagsMask();
          outer = entry.outerClass().map(ClassEntry::asInternalName).orElse(null);
          local = outer == null;
        }
      }
    }
    Set<String> exports = root.exports();
    int slash = name.lastIndexOf('/');
    String pkg = slash < 0 ? "" : name.substring(0, slash).replace('/', '.');
    boolean exported =
        exports == null || exports.contains(pkg.replace('.', '/')) || exports.contains(pkg);
    return new Info(name, model, access, outer, local, exported);
  }

  private Optional<Info> find(String name) {
    Info mine = own.get(name);
    if (mine != null) {
      return Optional.of(mine);
    }
    return resolved.computeIfAbsent(
        name,
        n -> {
          for (Root root : classpath) {
            byte[] bytes = root.read(n + ".class");
            if (bytes != null) {
              return Optional.of(info(ClassFile.of().parse(bytes), root));
            }
          }
          return Optional.empty();
        });
  }

  private static boolean isMarked(AttributedElement element) {
    return annotations(element).anyMatch(a -> a.className().stringValue().equals(INTERNAL));
  }

  private static Stream<Annotation> annotations(AttributedElement element) {
    return Stream.concat(
        element
            .findAttribute(Attributes.runtimeInvisibleAnnotations())
            .map(RuntimeInvisibleAnnotationsAttribute::annotations)
            .orElse(List.of())
            .stream(),
        element
            .findAttribute(Attributes.runtimeVisibleAnnotations())
            .map(RuntimeVisibleAnnotationsAttribute::annotations)
            .orElse(List.of())
            .stream());
  }

  /** Whether application code may name this type: public API in the sense of the README. */
  private boolean isApiType(Info info) {
    if (info.local() || info.has(AccessFlag.SYNTHETIC) || info.marked() || !info.exported()) {
      return false;
    }
    if (info.outer() == null) {
      return info.has(AccessFlag.PUBLIC);
    }
    Optional<Info> outer = find(info.outer());
    if (outer.isEmpty() || !isApiType(outer.get())) {
      return false;
    }
    return info.has(AccessFlag.PUBLIC)
        || (info.has(AccessFlag.PROTECTED) && !outer.get().has(AccessFlag.FINAL));
  }

  // ── The dump ────────────────────────────────────────────────────────────────────────────────

  private List<String> dump() {
    List<String> lines = new ArrayList<>();
    for (Info info : own.values()) {
      if (!isApiType(info)) {
        continue;
      }
      if (!lines.isEmpty()) {
        lines.add("");
      }
      lines.add(header(info));
      lines.addAll(members(info).keySet());
    }
    return lines;
  }

  private static String header(Info info) {
    ClassModel model = info.model();
    StringBuilder line = new StringBuilder();
    modifier(line, info.has(AccessFlag.PUBLIC), "public");
    modifier(line, info.has(AccessFlag.PROTECTED), "protected");
    modifier(line, info.outer() != null && info.has(AccessFlag.STATIC), "static");
    boolean iface = info.has(AccessFlag.INTERFACE);
    modifier(line, !iface && info.has(AccessFlag.ABSTRACT), "abstract");
    modifier(line, info.has(AccessFlag.FINAL), "final");
    Optional<PermittedSubclassesAttribute> permits =
        model.findAttribute(Attributes.permittedSubclasses());
    modifier(line, permits.isPresent(), "sealed");
    String kind =
        info.has(AccessFlag.ANNOTATION)
            ? "@interface"
            : iface
                ? "interface"
                : info.has(AccessFlag.ENUM)
                    ? "enum"
                    : model.findAttribute(Attributes.record()).isPresent() ? "record" : "class";
    line.append(kind).append(' ').append(binary(info.name()));
    ClassSignature signature = classSignature(model);
    line.append(typeParameters(signature.typeParameters()));
    String superclass = type(signature.superclassSignature());
    List<String> interfaces =
        signature.superinterfaceSignatures().stream().map(ApiSurfaceCheck::type).toList();
    boolean implicitSuperclass =
        superclass.equals("java.lang.Object")
            || superclass.equals("java.lang.Record")
            || superclass.startsWith("java.lang.Enum<");
    if (!iface && !implicitSuperclass) {
      line.append(" extends ").append(superclass);
    }
    if (!interfaces.isEmpty() && !info.has(AccessFlag.ANNOTATION)) {
      line.append(iface ? " extends " : " implements ").append(String.join(", ", interfaces));
    }
    permits.ifPresent(
        p ->
            line.append(" permits ")
                .append(
                    p.permittedSubclasses().stream()
                        .map(c -> binary(c.asInternalName()))
                        .sorted()
                        .collect(Collectors.joining(", "))));
    return line.toString();
  }

  private static ClassSignature classSignature(ClassModel model) {
    Optional<SignatureAttribute> attribute = model.findAttribute(Attributes.signature());
    if (attribute.isPresent()) {
      return ClassSignature.parseFrom(attribute.get().signature().stringValue());
    }
    StringBuilder raw = new StringBuilder();
    raw.append(
        model.superclass().map(c -> "L" + c.asInternalName() + ";").orElse("Ljava/lang/Object;"));
    model.interfaces().forEach(c -> raw.append("L").append(c.asInternalName()).append(";"));
    return ClassSignature.parseFrom(raw.toString());
  }

  /** The listed members of an API type, each with the StreamRune types its signature names. */
  private Map<String, Set<String>> members(Info info) {
    Map<String, Set<String>> members = new TreeMap<>();
    boolean finalType = info.has(AccessFlag.FINAL);
    boolean iface = info.has(AccessFlag.INTERFACE);
    for (FieldModel field : info.model().fields()) {
      AccessFlags flags = field.flags();
      if (!visible(flags, finalType) || flags.has(AccessFlag.SYNTHETIC) || isMarked(field)) {
        continue;
      }
      StringBuilder line = new StringBuilder("  ");
      modifiers(line, flags);
      Signature type =
          Signature.parseFrom(
              field
                  .findAttribute(Attributes.signature())
                  .map(s -> s.signature().stringValue())
                  .orElse(field.fieldType().stringValue()));
      line.append(type(type)).append(' ').append(field.fieldName().stringValue());
      field
          .findAttribute(Attributes.constantValue())
          .map(ConstantValueAttribute::constant)
          .ifPresent(
              c ->
                  line.append(" = ")
                      .append(
                          c instanceof StringEntry s
                              ? quote(s.stringValue())
                              : String.valueOf(c.constantValue())));
      Set<String> refs = new LinkedHashSet<>();
      collect(type, refs);
      members.put(line.toString(), refs);
    }
    for (MethodModel method : info.model().methods()) {
      AccessFlags flags = method.flags();
      String name = method.methodName().stringValue();
      if (!visible(flags, finalType)
          || flags.has(AccessFlag.SYNTHETIC)
          || flags.has(AccessFlag.BRIDGE)
          || name.equals("<clinit>")
          || isMarked(method)) {
        continue;
      }
      MethodSignature signature =
          MethodSignature.parseFrom(
              method
                  .findAttribute(Attributes.signature())
                  .map(s -> s.signature().stringValue())
                  .orElse(method.methodType().stringValue()));
      StringBuilder line = new StringBuilder("  ");
      modifiers(line, flags);
      modifier(
          line,
          iface && !flags.has(AccessFlag.ABSTRACT) && !flags.has(AccessFlag.STATIC),
          "default");
      String typeParameters = typeParameters(signature.typeParameters());
      if (!typeParameters.isEmpty()) {
        line.append(typeParameters).append(' ');
      }
      if (!name.equals("<init>")) {
        line.append(type(signature.result())).append(' ');
      }
      List<String> arguments = new ArrayList<>();
      for (Signature argument : signature.arguments()) {
        arguments.add(type(argument));
      }
      if (flags.has(AccessFlag.VARARGS) && !arguments.isEmpty()) {
        String last = arguments.removeLast();
        arguments.add(last.substring(0, last.length() - 2) + "...");
      }
      line.append(name).append('(').append(String.join(", ", arguments)).append(')');
      List<String> thrown = new ArrayList<>();
      Set<String> refs = new LinkedHashSet<>();
      if (!signature.throwableSignatures().isEmpty()) {
        for (Signature.ThrowableSig t : signature.throwableSignatures()) {
          thrown.add(type(t));
          collect(t, refs);
        }
      } else {
        method
            .findAttribute(Attributes.exceptions())
            .map(ExceptionsAttribute::exceptions)
            .orElse(List.of())
            .forEach(
                e -> {
                  thrown.add(binary(e.asInternalName()));
                  if (e.asInternalName().startsWith(OWN_PREFIX)) {
                    refs.add(e.asInternalName());
                  }
                });
      }
      if (!thrown.isEmpty()) {
        line.append(" throws ").append(String.join(", ", thrown));
      }
      signature.typeParameters().forEach(p -> collect(p, refs));
      signature.arguments().forEach(a -> collect(a, refs));
      collect(signature.result(), refs);
      members.put(line.toString(), refs);
    }
    return members;
  }

  private static boolean visible(AccessFlags flags, boolean finalType) {
    return flags.has(AccessFlag.PUBLIC) || (flags.has(AccessFlag.PROTECTED) && !finalType);
  }

  private static void modifiers(StringBuilder line, AccessFlags flags) {
    modifier(line, flags.has(AccessFlag.PUBLIC), "public");
    modifier(line, flags.has(AccessFlag.PROTECTED), "protected");
    modifier(line, flags.has(AccessFlag.STATIC), "static");
    modifier(line, flags.has(AccessFlag.ABSTRACT), "abstract");
    modifier(line, flags.has(AccessFlag.FINAL), "final");
  }

  private static void modifier(StringBuilder line, boolean present, String word) {
    if (present) {
      line.append(word).append(' ');
    }
  }

  private static String quote(String value) {
    StringBuilder quoted = new StringBuilder("\"");
    for (char c : value.toCharArray()) {
      switch (c) {
        case '"' -> quoted.append("\\\"");
        case '\\' -> quoted.append("\\\\");
        case '\n' -> quoted.append("\\n");
        case '\r' -> quoted.append("\\r");
        case '\t' -> quoted.append("\\t");
        default -> {
          if (c < 0x20 || c > 0x7e) {
            quoted.append(String.format("\\u%04x", (int) c));
          } else {
            quoted.append(c);
          }
        }
      }
    }
    return quoted.append('"').toString();
  }

  // ── Signatures ──────────────────────────────────────────────────────────────────────────────

  private static String binary(String internalName) {
    return internalName.replace('/', '.');
  }

  private static String typeParameters(List<Signature.TypeParam> parameters) {
    if (parameters.isEmpty()) {
      return "";
    }
    List<String> rendered = new ArrayList<>();
    for (Signature.TypeParam p : parameters) {
      List<String> bounds = new ArrayList<>();
      p.classBound().map(ApiSurfaceCheck::type).ifPresent(bounds::add);
      p.interfaceBounds().forEach(b -> bounds.add(type(b)));
      bounds.remove("java.lang.Object");
      rendered.add(
          p.identifier() + (bounds.isEmpty() ? "" : " extends " + String.join(" & ", bounds)));
    }
    return "<" + String.join(", ", rendered) + ">";
  }

  private static String type(Signature signature) {
    return switch (signature) {
      case Signature.BaseTypeSig base -> primitive(base.baseType());
      case Signature.TypeVarSig var -> var.identifier();
      case Signature.ArrayTypeSig array -> type(array.componentSignature()) + "[]";
      case Signature.ClassTypeSig cls -> {
        String name =
            cls.outerType().isPresent()
                ? type(cls.outerType().get()) + "$" + cls.className()
                : binary(cls.className());
        yield cls.typeArgs().isEmpty()
            ? name
            : name
                + "<"
                + cls.typeArgs().stream()
                    .map(ApiSurfaceCheck::typeArgument)
                    .collect(Collectors.joining(", "))
                + ">";
      }
      default -> throw new IllegalStateException("unexpected signature " + signature);
    };
  }

  private static String typeArgument(Signature.TypeArg argument) {
    return switch (argument) {
      case Signature.TypeArg.Unbounded u -> "?";
      case Signature.TypeArg.Bounded b ->
          switch (b.wildcardIndicator()) {
            case NONE -> type(b.boundType());
            case EXTENDS -> "? extends " + type(b.boundType());
            case SUPER -> "? super " + type(b.boundType());
          };
    };
  }

  private static String primitive(char c) {
    return switch (c) {
      case 'B' -> "byte";
      case 'C' -> "char";
      case 'D' -> "double";
      case 'F' -> "float";
      case 'I' -> "int";
      case 'J' -> "long";
      case 'S' -> "short";
      case 'Z' -> "boolean";
      case 'V' -> "void";
      default -> throw new IllegalStateException("unexpected base type " + c);
    };
  }

  /** Adds the internal names of the StreamRune classes a signature names. */
  private static void collect(Signature signature, Set<String> refs) {
    switch (signature) {
      case Signature.ArrayTypeSig array -> collect(array.componentSignature(), refs);
      case Signature.ClassTypeSig cls -> {
        cls.outerType().ifPresent(o -> collect(o, refs));
        String name =
            cls.outerType().isPresent()
                ? internal(cls.outerType().get()) + "$" + cls.className()
                : cls.className();
        if (name.startsWith(OWN_PREFIX)) {
          refs.add(name);
        }
        for (Signature.TypeArg argument : cls.typeArgs()) {
          if (argument instanceof Signature.TypeArg.Bounded b) {
            collect(b.boundType(), refs);
          }
        }
      }
      default -> {}
    }
  }

  private static void collect(Signature.TypeParam parameter, Set<String> refs) {
    parameter.classBound().ifPresent(b -> collect(b, refs));
    parameter.interfaceBounds().forEach(b -> collect(b, refs));
  }

  private static String internal(Signature.ClassTypeSig cls) {
    return cls.outerType().isPresent()
        ? internal(cls.outerType().get()) + "$" + cls.className()
        : cls.className();
  }

  // ── The @Internal rules ─────────────────────────────────────────────────────────────────────

  /** Every listed signature that names a StreamRune type application code may not name. */
  private List<String> exposures() {
    List<String> violations = new ArrayList<>();
    for (Info info : own.values()) {
      if (!isApiType(info)) {
        continue;
      }
      String header = header(info);
      Set<String> headerRefs = new LinkedHashSet<>();
      ClassSignature signature = classSignature(info.model());
      signature.typeParameters().forEach(p -> collect(p, headerRefs));
      collect(signature.superclassSignature(), headerRefs);
      signature.superinterfaceSignatures().forEach(s -> collect(s, headerRefs));
      check(header, headerRefs, violations);
      members(info)
          .forEach((line, refs) -> check(header + " :: " + line.strip(), refs, violations));
    }
    return violations;
  }

  private void check(String where, Set<String> refs, List<String> violations) {
    for (String ref : refs) {
      Optional<Info> target = find(ref);
      if (target.isEmpty()) {
        unresolved.add(binary(ref));
        continue;
      }
      String reason = notApiReason(target.get());
      if (reason != null) {
        violations.add(where + "\n      names " + binary(ref) + ", " + reason);
      }
    }
  }

  private String notApiReason(Info info) {
    if (info.marked()) {
      return "which is marked @Internal";
    }
    if (!info.exported()) {
      return "whose package its module does not export";
    }
    if (info.outer() != null) {
      Optional<Info> outer = find(info.outer());
      if (outer.isPresent()) {
        String reason = notApiReason(outer.get());
        if (reason != null) {
          return "nested in " + binary(info.outer()) + ", " + reason;
        }
      }
      return info.has(AccessFlag.PUBLIC) || info.has(AccessFlag.PROTECTED)
          ? null
          : "which is not public";
    }
    return info.has(AccessFlag.PUBLIC) ? null : "which is not public";
  }

  /** Every declaration marked {@code @Internal} without {@code @hidden} in its javadoc. */
  static List<String> hiddenViolations(Path sources) throws IOException {
    List<String> violations = new ArrayList<>();
    if (!Files.isDirectory(sources)) {
      return violations;
    }
    List<Path> files;
    try (Stream<Path> walk = Files.walk(sources)) {
      files = walk.filter(f -> f.toString().endsWith(".java")).sorted().toList();
    }
    for (Path file : files) {
      List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
      // A plain @Internal is StreamRune's only where the file imports it or lives beside it; a
      // framework's annotation of the same simple name is not checked.
      boolean ownSimpleName =
          lines.stream()
              .map(String::strip)
              .anyMatch(
                  l ->
                      l.equals("import org.streamrune.core.Internal;")
                          || l.equals("package org.streamrune.core;"));
      for (int i = 0; i < lines.size(); i++) {
        String line = lines.get(i).strip();
        if (!isInternalAnnotation(line, ownSimpleName)) {
          continue;
        }
        int j = i - 1;
        while (j >= 0 && (lines.get(j).isBlank() || lines.get(j).strip().startsWith("@"))) {
          j--;
        }
        boolean hidden = false;
        if (j >= 0 && lines.get(j).strip().endsWith("*/")) {
          for (int k = j; k >= 0; k--) {
            if (lines.get(k).contains("@hidden")) {
              hidden = true;
            }
            if (lines.get(k).contains("/**") || lines.get(k).contains("/*")) {
              break;
            }
          }
        }
        if (!hidden) {
          violations.add(
              sources.relativize(file)
                  + ":"
                  + (i + 1)
                  + " is marked @Internal but its javadoc has no @hidden tag");
        }
      }
    }
    return violations;
  }

  private static boolean isInternalAnnotation(String line, boolean ownSimpleName) {
    List<String> markers =
        ownSimpleName
            ? List.of("@Internal", "@org.streamrune.core.Internal")
            : List.of("@org.streamrune.core.Internal");
    for (String marker : markers) {
      if (line.startsWith(marker)) {
        String rest = line.substring(marker.length());
        if (rest.isEmpty() || rest.charAt(0) == ' ' || rest.charAt(0) == '(') {
          return true;
        }
      }
    }
    return false;
  }

  // ── Arguments and the difference ────────────────────────────────────────────────────────────

  private static Map<String, List<String>> parse(String[] args) {
    Map<String, List<String>> options = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (!args[i].startsWith("--") || i + 1 >= args.length) {
        throw new IllegalArgumentException("expected --option value at " + args[i]);
      }
      options.computeIfAbsent(args[i], k -> new ArrayList<>()).add(args[++i]);
    }
    return options;
  }

  private static String one(Map<String, List<String>> options, String name) {
    List<String> values = options.get(name);
    if (values == null || values.size() != 1) {
      throw new IllegalArgumentException(name + " is required once");
    }
    return values.getFirst();
  }

  private static String difference(String module, Path baseline, String expected, String actual) {
    Set<String> was = new LinkedHashSet<>(expected.lines().toList());
    Set<String> is = new LinkedHashSet<>(actual.lines().toList());
    StringBuilder text =
        new StringBuilder("The public API of ")
            .append(module)
            .append(" differs from ")
            .append(baseline)
            .append(".\n");
    String type = null;
    List<String> removed = new ArrayList<>();
    for (String line : was) {
      if (!line.startsWith(" ")) {
        type = line;
      }
      if (!line.isBlank() && !is.contains(line)) {
        removed.add(line.startsWith(" ") ? "- " + line + "    (in " + type + ")" : "- " + line);
      }
    }
    List<String> added = new ArrayList<>();
    for (String line : is) {
      if (!line.startsWith(" ")) {
        type = line;
      }
      if (!line.isBlank() && !was.contains(line)) {
        added.add(line.startsWith(" ") ? "+ " + line + "    (in " + type + ")" : "+ " + line);
      }
    }
    if (!removed.isEmpty()) {
      text.append(
              "Removed or changed (code compiled or written against the API in the file breaks):\n")
          .append(String.join("\n", removed))
          .append('\n');
    }
    if (!added.isEmpty()) {
      text.append("Added:\n").append(String.join("\n", added)).append('\n');
    }
    return text.append(
            "If the change is intended, run ./gradlew apiDump and commit the rewritten file.")
        .toString();
  }
}
