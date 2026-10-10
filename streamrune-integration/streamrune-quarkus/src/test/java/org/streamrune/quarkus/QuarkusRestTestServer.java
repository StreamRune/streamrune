package org.streamrune.quarkus;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jboss.jandex.Index;
import org.jboss.resteasy.reactive.server.core.reflection.ReflectiveContextInjectedBeanFactory;
import org.jboss.resteasy.reactive.server.processor.ResteasyReactiveDeploymentManager;
import org.jboss.resteasy.reactive.server.processor.scanning.AsyncReturnTypeScanner;
import org.jboss.resteasy.reactive.server.spi.DefaultRuntimeConfiguration;
import org.jboss.resteasy.reactive.server.vertx.ResteasyReactiveVertxHandler;
import org.jboss.resteasy.reactive.server.vertx.VertxRequestContextFactory;
import org.jboss.resteasy.reactive.spi.BeanFactory;

/**
 * Serves one JAX-RS resource through Quarkus REST over HTTP, without a Quarkus application.
 *
 * <p>A {@code @QuarkusTest} needs the Quarkus Gradle plugin, which this library module does not
 * apply. Quarkus REST itself runs without it: {@link ResteasyReactiveDeploymentManager} scans the
 * resource the way the Quarkus build does (the same endpoint indexer reads {@code @Produces} and
 * {@code @RestStreamElementType}), and the result is deployed on a Vert.x HTTP server with the same
 * request handlers a Quarkus application uses. A response therefore goes through the real Quarkus
 * REST serialization, including the Server-Sent Events writer and the lookup of a {@link
 * jakarta.ws.rs.ext.MessageBodyWriter} for each frame.
 *
 * <p>The resource is the instance handed in, so a test builds it with its own collaborators. The
 * message body readers and writers are the Quarkus REST built-ins plus the {@code @Provider}
 * classes named by the test — the stand-in for the JSON extension an application chooses.
 */
final class QuarkusRestTestServer implements AutoCloseable {

  private final Vertx vertx;
  private final ExecutorService workers;
  private final ResteasyReactiveDeploymentManager.RunnableApplication application;
  private final int port;

  private QuarkusRestTestServer(
      Vertx vertx,
      ExecutorService workers,
      ResteasyReactiveDeploymentManager.RunnableApplication application,
      int port) {
    this.vertx = vertx;
    this.workers = workers;
    this.application = application;
    this.port = port;
  }

  /**
   * Deploys {@code resource} and starts the HTTP server on a free port.
   *
   * @param resource the resource instance that serves the requests
   * @param providers the {@code @Provider} classes of the application (each with a no-argument
   *     constructor)
   * @return the running server
   * @throws Exception if the resource cannot be scanned or the server cannot start
   */
  static QuarkusRestTestServer start(Object resource, Class<?>... providers) throws Exception {
    List<Class<?>> indexed = new ArrayList<>(List.of(providers));
    indexed.add(resource.getClass());
    ResteasyReactiveDeploymentManager.ScanStep scan =
        ResteasyReactiveDeploymentManager.start(Index.of(indexed.toArray(Class<?>[]::new)));
    // The scanner Quarkus registers for reactive return types; it is what serves a Multi.
    scan.addMethodScanner(new AsyncReturnTypeScanner());
    ResteasyReactiveDeploymentManager.PreparedApplication prepared =
        scan.scan()
            .prepare(
                QuarkusRestTestServer.class.getClassLoader(),
                className ->
                    className.equals(resource.getClass().getName())
                        ? existingInstance(resource)
                        : ReflectiveContextInjectedBeanFactory.STRING_FACTORY.apply(className));
    prepared.addScannedSerializers();
    prepared.addBuiltinSerializers();

    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    Vertx vertx = Vertx.vertx();
    try {
      ResteasyReactiveDeploymentManager.RunnableApplication application =
          prepared.createApplication(
              new DefaultRuntimeConfiguration(
                  Duration.ofMinutes(1),
                  true,
                  System.getProperty("java.io.tmpdir"),
                  List.of(),
                  StandardCharsets.UTF_8,
                  OptionalLong.empty(),
                  2048,
                  1000),
              new VertxRequestContextFactory(),
              workers);
      Router router = Router.router(vertx);
      router
          .route()
          .handler(new ResteasyReactiveVertxHandler(_ -> {}, application.getInitialHandler()));
      HttpServer server =
          vertx
              .createHttpServer()
              .requestHandler(router)
              .listen(0)
              .toCompletionStage()
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);
      return new QuarkusRestTestServer(vertx, workers, application, server.actualPort());
    } catch (Exception e) {
      vertx.close();
      workers.shutdownNow();
      throw e;
    }
  }

  /** The port the server listens on. */
  int port() {
    return port;
  }

  @Override
  public void close() throws Exception {
    try {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    } finally {
      application.close();
      workers.shutdownNow();
    }
  }

  private static BeanFactory<Object> existingInstance(Object resource) {
    return () ->
        new BeanFactory.BeanInstance<>() {
          @Override
          public Object getInstance() {
            return resource;
          }

          @Override
          public void close() {
            // the test owns the resource
          }
        };
  }
}
