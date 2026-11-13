import com.sun.source.tree.AnnotationTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ImportTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.ModifiersTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.DocTrees;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreePath;
import com.sun.source.util.TreePathScanner;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.classfile.AccessFlags;
import java.lang.classfile.Annotation;
import java.lang.classfile.AnnotationElement;
import java.lang.classfile.AnnotationValue;
import java.lang.classfile.AttributedElement;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.ClassSignature;
import java.lang.classfile.FieldModel;
import java.lang.classfile.MethodModel;
import java.lang.classfile.MethodSignature;
import java.lang.classfile.Signature;
import java.lang.classfile.attribute.AnnotationDefaultAttribute;
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
import java.lang.classfile.attribute.SourceFileAttribute;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.StringEntry;
import java.lang.reflect.AccessFlag;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

/**
 * The public API of one published module, written to and compared with the module's checked-in
 * {@code api/<module>.api} file, plus the rules of the {@code @Internal} marker. The root build
 * runs it on the module's compiled classes through two tasks of every published module: {@code
 * verifyApiSurface} (part of {@code check}) and {@code apiDump}; {@code ApiSurfaceCheckTest.java}
 * beside it tests it ({@code testApiSurfaceCheck}, part of {@code check}).
 *
 * <p><b>What the file lists.</b> Every public type of the module that application code can reach
 * (public, or protected inside a public non-final type; in an exported package when the module has
 * a {@code module-info}; not marked {@code @Internal} and not nested in such a type), with its
 * kind, modifiers, type parameters, supertypes, permitted subtypes and, for an annotation type, its
 * {@code @Retention}, {@code @Target}, {@code @Repeatable} and {@code @Inherited}; the order of an
 * enum's constants; and its public members (and protected members of a non-final type) that are not
 * marked {@code @Internal}: constructors, methods with their generic signatures, thrown exceptions
 * and annotation defaults, fields with their constant values, each marked when it is
 * {@code @Deprecated}. A change to any of it changes the file, so a change to the public API is
 * made on purpose and is visible in review: {@code verifyApiSurface} fails until {@code ./gradlew
 * apiDump} rewrites the file. Its message lists, type by type, the lines that were removed or
 * changed apart from the lines that were added.
 *
 * <p><b>The {@code @Internal} rules</b> (both tasks fail on a violation):
 *
 * <ul>
 *   <li>no listed signature names a StreamRune type that is not public API: a type marked
 *       {@code @Internal} or nested in one, a type in a package its module does not export, a type
 *       application code cannot reach, or a type not on the compile class path;
 *   <li>every declaration marked {@code @Internal} carries the {@code @hidden} javadoc tag, so the
 *       generated API documentation leaves it out. The sources are parsed with the JDK's compiler
 *       tree API, and per source file the marked declarations found there must be as many as the
 *       class files mark, so a marker the source reading cannot see fails too.
 * </ul>
 *
 * <p>Arguments: {@code --mode check|dump}, {@code --module <gradle path>}, {@code --baseline
 * <file>}, {@code --report <file>}, and one or more {@code --classes <dir>}, {@code --classpath
 * <dir or jar>} and {@code --sources <dir>}. It needs nothing but the JDK and runs with the source
 * launcher ({@code java ApiSurfaceCheck.java ...}).
 */
public final class ApiSurfaceCheck {

  private static final String INTERNAL = "Lorg/streamrune/core/Internal;";
  private static final String INTERNAL_NAME = "org.streamrune.core.Internal";
  private static final String OWN_PREFIX = "org/streamrune/";
  private static final List<String> KINDS =
      List.of("class", "interface", "enum", "record", "@interface");
  private static final Map<String, String> META_ANNOTATIONS =
      Map.of(
          "Ljava/lang/annotation/Retention;", "@Retention",
          "Ljava/lang/annotation/Target;", "@Target",
          "Ljava/lang/annotation/Repeatable;", "@Repeatable",
          "Ljava/lang/annotation/Inherited;", "@Inherited");
  private static final List<String> META_ORDER =
      List.of("@Retention", "@Target", "@Repeatable", "@Inherited");

  private final Map<String, Info> own = new TreeMap<>();
  private final List<Root> classpath = new ArrayList<>();
  private final Map<String, Optional<Info>> resolved = new HashMap<>();

  public static void main(String[] args) throws IOException {
    int status = run(args, System.out, System.err);
    if (status != 0) {
      System.exit(status);
    }
  }

  /** Runs the check or the dump; returns the exit status (0 when everything holds). */
  static int run(String[] args, PrintStream out, PrintStream err) throws IOException {
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
    violations.addAll(
        check.hiddenViolations(
            options.getOrDefault("--sources", List.of()).stream().map(Path::of).toList()));
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
      out.println(module + ": wrote " + baseline);
    } else {
      String expected =
          Files.exists(baseline)
              ? Files.readString(baseline, StandardCharsets.UTF_8).replace("\r\n", "\n")
              : null;
      if (expected == null) {
        err.println(
            module
                + " has no API baseline at "
                + baseline
                + ". Run ./gradlew apiDump and commit the file.");
        failed = true;
      } else if (!expected.equals(text)) {
        err.println(difference(module, baseline, expected, text));
        failed = true;
      }
    }
    if (!violations.isEmpty()) {
      err.println(
          module
              + " breaks the rules of the @Internal marker (README, \"API stability\"):\n  "
              + String.join("\n  ", violations));
      failed = true;
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
    return failed ? 1 : 0;
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

    /** The source file, as a path below a source root: package directories and file name. */
    String sourcePath() {
      int slash = name.lastIndexOf('/');
      String dir = slash < 0 ? "" : name.substring(0, slash + 1);
      return model
          .findAttribute(Attributes.sourceFile())
          .map(SourceFileAttribute::sourceFile)
          .map(f -> dir + f.stringValue())
          .orElse(dir + "?");
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

    /** The packages exported to everyone (internal form), or null without a module descriptor. */
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
      for (Path file : files.filter(f -> f.toString().endsWith(".class")).sorted().toList()) {
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
    String pkg = slash < 0 ? "" : name.substring(0, slash);
    return new Info(name, model, access, outer, local, exports == null || exports.contains(pkg));
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

  private static boolean isDeprecated(AttributedElement element) {
    return annotations(element)
        .anyMatch(a -> a.className().stringValue().equals("Ljava/lang/Deprecated;"));
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
    return !info.local() && !info.has(AccessFlag.SYNTHETIC) && notApiReason(info) == null;
  }

  /** Why application code may not name this type, or null when it may. */
  private String notApiReason(Info info) {
    if (info.marked()) {
      return "which is marked @Internal";
    }
    if (!info.exported()) {
      return "whose package its module does not export";
    }
    if (info.outer() == null) {
      return info.has(AccessFlag.PUBLIC) ? null : "which is not public";
    }
    Optional<Info> outer = find(info.outer());
    if (outer.isEmpty()) {
      return "nested in " + binary(info.outer()) + ", which is not on the compile class path";
    }
    String reason = notApiReason(outer.get());
    if (reason != null) {
      return "nested in " + binary(info.outer()) + ", " + reason;
    }
    if (info.has(AccessFlag.PUBLIC)) {
      return null;
    }
    return info.has(AccessFlag.PROTECTED) && !outer.get().has(AccessFlag.FINAL)
        ? null
        : "which application code cannot reach (not public, or protected in a final type)";
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
    boolean annotation = info.has(AccessFlag.ANNOTATION);
    boolean iface = info.has(AccessFlag.INTERFACE);
    boolean isEnum = info.has(AccessFlag.ENUM);
    if (annotation) {
      Map<String, String> meta = new TreeMap<>();
      annotations(model)
          .filter(a -> META_ANNOTATIONS.containsKey(a.className().stringValue()))
          .forEach(a -> meta.put(META_ANNOTATIONS.get(a.className().stringValue()), elements(a)));
      for (String name : META_ORDER) {
        if (meta.containsKey(name)) {
          line.append(name).append(meta.get(name)).append(' ');
        }
      }
    }
    modifier(line, isDeprecated(model), "@Deprecated");
    modifier(line, info.has(AccessFlag.PUBLIC), "public");
    modifier(line, info.has(AccessFlag.PROTECTED), "protected");
    modifier(line, info.outer() != null && info.has(AccessFlag.STATIC), "static");
    // An enum's abstract, final and sealed flags follow from whether its constants have bodies.
    modifier(line, !iface && !isEnum && info.has(AccessFlag.ABSTRACT), "abstract");
    modifier(line, !isEnum && info.has(AccessFlag.FINAL), "final");
    Optional<PermittedSubclassesAttribute> permits =
        isEnum ? Optional.empty() : model.findAttribute(Attributes.permittedSubclasses());
    modifier(line, permits.isPresent(), "sealed");
    String kind =
        annotation
            ? "@interface"
            : iface
                ? "interface"
                : isEnum
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
    if (!interfaces.isEmpty() && !annotation) {
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
    boolean innerClass =
        info.outer() != null && !info.has(AccessFlag.STATIC) && !iface && !info.local();
    List<String> constants = new ArrayList<>();
    for (FieldModel field : info.model().fields()) {
      AccessFlags flags = field.flags();
      if (!visible(flags, finalType) || flags.has(AccessFlag.SYNTHETIC) || isMarked(field)) {
        continue;
      }
      if (flags.has(AccessFlag.ENUM)) {
        constants.add(field.fieldName().stringValue());
      }
      StringBuilder line = new StringBuilder("  ");
      modifier(line, isDeprecated(field), "@Deprecated");
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
    if (!constants.isEmpty()) {
      // Declaration order: the ordinals, which a reordering changes.
      members.put("  constants " + String.join(", ", constants), new LinkedHashSet<>());
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
      Optional<String> generic =
          method.findAttribute(Attributes.signature()).map(s -> s.signature().stringValue());
      MethodSignature signature =
          MethodSignature.parseFrom(generic.orElse(method.methodType().stringValue()));
      List<Signature> parameters = new ArrayList<>(signature.arguments());
      if (name.equals("<init>") && innerClass && generic.isEmpty() && !parameters.isEmpty()) {
        // The descriptor of an inner class's constructor starts with the enclosing instance,
        // which its source does not declare (and a generic signature leaves out).
        parameters.removeFirst();
      }
      StringBuilder line = new StringBuilder("  ");
      modifier(line, isDeprecated(method), "@Deprecated");
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
      for (Signature parameter : parameters) {
        arguments.add(type(parameter));
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
      method
          .findAttribute(Attributes.annotationDefault())
          .map(AnnotationDefaultAttribute::defaultValue)
          .ifPresent(v -> line.append(" default ").append(value(v)));
      signature.typeParameters().forEach(p -> collect(p, refs));
      parameters.forEach(a -> collect(a, refs));
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

  /** The elements of an annotation, as written in source: {@code (A, B)} or nothing. */
  private static String elements(Annotation annotation) {
    List<AnnotationElement> elements = annotation.elements();
    if (elements.isEmpty()) {
      return "";
    }
    if (elements.size() == 1 && elements.getFirst().name().stringValue().equals("value")) {
      return "(" + value(elements.getFirst().value()) + ")";
    }
    return elements.stream()
        .map(e -> e.name().stringValue() + " = " + value(e.value()))
        .collect(Collectors.joining(", ", "(", ")"));
  }

  private static String value(AnnotationValue value) {
    return switch (value) {
      case AnnotationValue.OfString s -> quote(s.stringValue());
      case AnnotationValue.OfBoolean b -> String.valueOf(b.booleanValue());
      case AnnotationValue.OfChar c ->
          "'" + quote(String.valueOf(c.charValue())).replace("\"", "") + "'";
      case AnnotationValue.OfConstant c -> String.valueOf(c.resolvedValue());
      case AnnotationValue.OfClass c ->
          type(Signature.parseFrom(c.className().stringValue())) + ".class";
      case AnnotationValue.OfEnum e ->
          type(Signature.parseFrom(e.className().stringValue()))
              + "."
              + e.constantName().stringValue();
      case AnnotationValue.OfArray a ->
          a.values().stream()
              .map(ApiSurfaceCheck::value)
              .collect(Collectors.joining(", ", "{", "}"));
      case AnnotationValue.OfAnnotation a ->
          "@"
              + type(Signature.parseFrom(a.annotation().className().stringValue()))
              + elements(a.annotation());
    };
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
        String name = internal(cls);
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
      String reason =
          target.isEmpty()
              ? "which is not on the compile class path, so it cannot be checked"
              : notApiReason(target.get());
      if (reason != null) {
        violations.add(where + "\n      names " + binary(ref) + ", " + reason);
      }
    }
  }

  /**
   * Every declaration marked {@code @Internal} without {@code @hidden} in its javadoc, and every
   * source file whose marked declarations the parsed source and the class files count differently.
   */
  private List<String> hiddenViolations(List<Path> sourceRoots) throws IOException {
    Map<String, List<String>> compiled = new TreeMap<>();
    for (Info info : own.values()) {
      List<String> marked = compiled.computeIfAbsent(info.sourcePath(), k -> new ArrayList<>());
      if (info.marked()) {
        marked.add(binary(info.name()));
      }
      for (FieldModel field : info.model().fields()) {
        if (!field.flags().has(AccessFlag.SYNTHETIC) && isMarked(field)) {
          marked.add(binary(info.name()) + "." + field.fieldName().stringValue());
        }
      }
      for (MethodModel method : info.model().methods()) {
        if (!method.flags().has(AccessFlag.SYNTHETIC) && isMarked(method)) {
          marked.add(binary(info.name()) + "." + method.methodName().stringValue() + "()");
        }
      }
    }
    Map<String, Path> files = new TreeMap<>();
    for (Path root : sourceRoots) {
      if (!Files.isDirectory(root)) {
        continue;
      }
      try (Stream<Path> walk = Files.walk(root)) {
        walk.filter(f -> f.toString().endsWith(".java"))
            .filter(f -> !f.getFileName().toString().equals("module-info.java"))
            .forEach(f -> files.put(root.relativize(f).toString().replace('\\', '/'), f));
      }
    }
    List<String> violations = new ArrayList<>();
    if (files.isEmpty()) {
      return violations;
    }
    Map<String, Integer> found = new TreeMap<>();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    try (StandardJavaFileManager manager =
        compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
      Map<JavaFileObject, String> paths = new LinkedHashMap<>();
      for (Map.Entry<String, Path> e : files.entrySet()) {
        paths.put(manager.getJavaFileObjects(e.getValue()).iterator().next(), e.getKey());
      }
      JavacTask task =
          (JavacTask)
              compiler.getTask(null, manager, d -> {}, List.of("-proc:none"), null, paths.keySet());
      DocTrees docs = DocTrees.instance(task);
      for (CompilationUnitTree unit : task.parse()) {
        String path = paths.get(unit.getSourceFile());
        boolean simpleName = seesInternal(unit);
        int[] count = {0};
        new TreePathScanner<Void, Void>() {
          @Override
          public Void visitClass(ClassTree tree, Void unused) {
            mark(tree.getModifiers(), tree.getSimpleName().toString());
            return super.visitClass(tree, unused);
          }

          @Override
          public Void visitMethod(MethodTree tree, Void unused) {
            mark(tree.getModifiers(), tree.getName() + "()");
            return null;
          }

          @Override
          public Void visitVariable(VariableTree tree, Void unused) {
            Tree parent = getCurrentPath().getParentPath().getLeaf();
            if (parent instanceof ClassTree) {
              mark(tree.getModifiers(), tree.getName().toString());
            }
            return null;
          }

          private void mark(ModifiersTree modifiers, String name) {
            if (modifiers.getAnnotations().stream().noneMatch(a -> isInternal(a, simpleName))) {
              return;
            }
            count[0]++;
            String doc = docs.getDocComment(getCurrentPath());
            if (doc == null || !doc.contains("@hidden")) {
              long line =
                  unit.getLineMap()
                      .getLineNumber(
                          docs.getSourcePositions()
                              .getStartPosition(unit, getCurrentPath().getLeaf()));
              violations.add(
                  path
                      + ":"
                      + line
                      + " "
                      + name
                      + " is marked @Internal but its javadoc has no @hidden tag");
            }
          }
        }.scan(new TreePath(unit), null);
        found.put(path, count[0]);
      }
    }
    for (Map.Entry<String, List<String>> e : compiled.entrySet()) {
      int inSource = found.getOrDefault(e.getKey(), 0);
      if (files.containsKey(e.getKey()) && inSource != e.getValue().size()) {
        violations.add(
            e.getKey()
                + ": the class files mark "
                + e.getValue().size()
                + " declaration(s) "
                + e.getValue()
                + " @Internal, the source reading found "
                + inSource);
      }
    }
    return violations;
  }

  /** Whether the simple name {@code Internal} in this file means StreamRune's marker. */
  private static boolean seesInternal(CompilationUnitTree unit) {
    String pkg = unit.getPackageName() == null ? "" : unit.getPackageName().toString();
    boolean otherSingle = false;
    boolean ownSingle = false;
    boolean ownWildcard = false;
    for (ImportTree tree : unit.getImports()) {
      if (tree.isStatic()) {
        continue;
      }
      String name = tree.getQualifiedIdentifier().toString();
      if (name.equals(INTERNAL_NAME)) {
        ownSingle = true;
      } else if (name.endsWith(".Internal")) {
        otherSingle = true;
      } else if (name.equals("org.streamrune.core.*")) {
        ownWildcard = true;
      }
    }
    return ownSingle || (!otherSingle && (ownWildcard || pkg.equals("org.streamrune.core")));
  }

  private static boolean isInternal(AnnotationTree annotation, boolean simpleName) {
    String name = annotation.getAnnotationType().toString();
    return name.equals(INTERNAL_NAME) || (simpleName && name.equals("Internal"));
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

  /** A dump read back: the header line of every type and its member lines, by type name. */
  private static Map<String, Map.Entry<String, List<String>>> types(String text) {
    Map<String, Map.Entry<String, List<String>>> types = new LinkedHashMap<>();
    List<String> members = null;
    for (String line : text.lines().toList()) {
      if (line.isBlank() || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith(" ")) {
        if (members != null) {
          members.add(line);
        }
        continue;
      }
      members = new ArrayList<>();
      types.put(typeName(line), Map.entry(line, members));
    }
    return types;
  }

  private static String typeName(String header) {
    String[] words = header.split(" ");
    for (int i = 0; i + 1 < words.length; i++) {
      if (KINDS.contains(words[i])) {
        String name = words[i + 1];
        int generic = name.indexOf('<');
        return generic < 0 ? name : name.substring(0, generic);
      }
    }
    return header;
  }

  static String difference(String module, Path baseline, String expected, String actual) {
    Map<String, Map.Entry<String, List<String>>> was = types(expected);
    Map<String, Map.Entry<String, List<String>>> is = types(actual);
    List<String> removed = new ArrayList<>();
    List<String> added = new ArrayList<>();
    Set<String> names = new TreeSet<>(was.keySet());
    names.addAll(is.keySet());
    for (String name : names) {
      Map.Entry<String, List<String>> before = was.get(name);
      Map.Entry<String, List<String>> after = is.get(name);
      if (after == null) {
        removed.add("- " + before.getKey() + "    (type removed)");
        continue;
      }
      if (before == null) {
        added.add("+ " + after.getKey() + "    (type added)");
        after.getValue().forEach(m -> added.add("+ " + m + "    (in " + name + ")"));
        continue;
      }
      if (!before.getKey().equals(after.getKey())) {
        removed.add("- " + before.getKey());
        added.add("+ " + after.getKey());
      }
      Set<String> beforeMembers = new LinkedHashSet<>(before.getValue());
      Set<String> afterMembers = new LinkedHashSet<>(after.getValue());
      for (String m : beforeMembers) {
        if (!afterMembers.contains(m)) {
          removed.add("- " + m + "    (in " + name + ")");
        }
      }
      for (String m : afterMembers) {
        if (!beforeMembers.contains(m)) {
          added.add("+ " + m + "    (in " + name + ")");
        }
      }
    }
    StringBuilder text =
        new StringBuilder("The public API of ")
            .append(module)
            .append(" differs from ")
            .append(baseline)
            .append(".\n");
    if (!removed.isEmpty()) {
      text.append(
              "Removed or changed (each can break code written against the API in the file,"
                  + " unless it widens what it declares):\n")
          .append(String.join("\n", removed))
          .append('\n');
    }
    if (!added.isEmpty()) {
      text.append("Added:\n").append(String.join("\n", added)).append('\n');
    }
    if (removed.isEmpty() && added.isEmpty()) {
      text.append("Only the order or the comments differ.\n");
    }
    return text.append(
            "If the change is intended, run ./gradlew apiDump and commit the rewritten file.")
        .toString();
  }
}
