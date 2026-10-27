package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.streamrune.core.Authorization;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.tck.ContractBeans;
import org.streamrune.integration.tck.ContractHttp;
import org.streamrune.integration.tck.HttpResponse;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.UnboundRequestContextQueryBus;
import org.streamrune.test.InMemoryEventStoreFactory;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A Spring MVC controller that returns a {@link Callable} runs its body on the MVC task executor,
 * outside the binding {@link ScopedValueFilter} made on the servlet thread, so {@link
 * StreamRuneContext#CURRENT} is unbound there. The command bus and the query bus the integration
 * builds take the caller from the request the thread serves ({@link
 * RequestAttributeContextSource}): a command the callable dispatches is audited as the request's
 * caller, and a query's handler sees that caller — on an embedded Tomcat, through the real filter
 * chain and async dispatch.
 */
class SpringMvcAsyncCallerTest {

  /** The servlet web stack and the StreamRune auto-configuration. */
  @Configuration(proxyBeanMethods = false)
  @ImportAutoConfiguration({
    PropertyPlaceholderAutoConfiguration.class,
    LifecycleAutoConfiguration.class,
    TomcatServletWebServerAutoConfiguration.class,
    DispatcherServletAutoConfiguration.class,
    WebMvcAutoConfiguration.class,
    HttpMessageConvertersAutoConfiguration.class,
    JacksonAutoConfiguration.class,
    StreamRuneAutoConfiguration.class
  })
  static class AsyncApplication {}

  private ConfigurableApplicationContext context;

  @AfterEach
  void stop() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  void aCommandACallableDispatchesIsAuditedAsTheRequestsCaller() {
    start();

    HttpResponse response =
        http(
            "POST",
            "/async/commands/order-1",
            Map.of(RequestIdentityPolicy.USER_ID_HEADER, "alice"));

    assertThat(response.status()).as("%s", response).isEqualTo(200);
    AsyncController controller = context.getBean(AsyncController.class);
    assertThat(controller.boundInTheCallable.get())
        .as("the callable runs outside the filter's binding")
        .isFalse();
    assertThat(controller.callableThread.get()).isNotEqualTo(controller.requestThread.get());
    assertThat(context.getBean(ContractBeans.RecordingAuditStore.class).entries())
        .singleElement()
        .satisfies(entry -> assertThat(entry.userId()).isEqualTo(UserId.of("alice")));
  }

  @Test
  void aQueryACallableDispatchesSeesTheRequestsCaller() {
    start();

    HttpResponse response =
        http("GET", "/async/queries/me", Map.of(RequestIdentityPolicy.USER_ID_HEADER, "bob"));

    assertThat(response.status()).as("%s", response).isEqualTo(200);
    assertThat(response.body()).isEqualTo("bob");
    assertThat(context.getBean(QueryBus.class)).isInstanceOf(UnboundRequestContextQueryBus.class);
  }

  /** A command dispatched outside any request stays anonymous: no request is current there. */
  @Test
  void aCommandDispatchedOutsideARequestStaysAnonymous() {
    start();

    context.getBean(CommandBus.class).execute(new ContractBeans.ContractCommand("order-2"));

    assertThat(context.getBean(ContractBeans.RecordingAuditStore.class).entries())
        .singleElement()
        .satisfies(entry -> assertThat(entry.userId()).isNull());
  }

  private void start() {
    context =
        new SpringApplicationBuilder(AsyncApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .initializers(
                applicationContext -> declare((GenericApplicationContext) applicationContext))
            .properties(
                "server.port=0",
                "spring.main.banner-mode=off",
                RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY + "=true")
            .run();
  }

  private static void declare(GenericApplicationContext context) {
    context.registerBean(
        EventStoreFactory.class, () -> InMemoryEventStoreFactory.builder().build());
    context.registerBean(OffsetStore.class, InMemoryOffsetStore::new);
    context.registerBean(ContractBeans.RecordingAuditStore.class);
    context.registerBean(
        "contractCommandRegistration",
        DeciderRegistration.class,
        () -> new ContractBeans.ContractCommandRegistration().get());
    context.registerBean(AsyncController.class);
  }

  private HttpResponse http(String method, String path, Map<String, String> headers) {
    int port = ((WebServerApplicationContext) context).getWebServer().getPort();
    return ContractHttp.send(port, method, path, headers, null);
  }

  /** Answers the caller its query handler sees. */
  record WhoAmI() implements Query<String> {}

  /** A controller whose handlers return a {@link Callable} and bind nothing themselves. */
  @RestController
  public static class AsyncController {

    private final CommandBus commandBus;
    private final QueryBus queryBus;
    final AtomicReference<Boolean> boundInTheCallable = new AtomicReference<>();
    final AtomicReference<Thread> requestThread = new AtomicReference<>();
    final AtomicReference<Thread> callableThread = new AtomicReference<>();

    public AsyncController(CommandBus commandBus, QueryBus queryBus) {
      this.commandBus = commandBus;
      this.queryBus = queryBus;
      queryBus.register(
          WhoAmI.class,
          query -> {
            UserId caller = Authorization.currentUserId();
            return caller == null ? "anonymous" : caller.value();
          });
    }

    @PostMapping("/async/commands/{id}")
    public Callable<String> dispatch(@PathVariable("id") String id) {
      requestThread.set(Thread.currentThread());
      return () -> {
        callableThread.set(Thread.currentThread());
        boundInTheCallable.set(StreamRuneContext.CURRENT.isBound());
        commandBus.execute(new ContractBeans.ContractCommand(id));
        return "executed";
      };
    }

    @GetMapping("/async/queries/me")
    public Callable<String> whoAmI() {
      return () -> queryBus.dispatch(new WhoAmI());
    }
  }
}
