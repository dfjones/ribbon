package com.netflix.ribbon.examples;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.IClientConfig;
import com.netflix.hystrix.contrib.rxnetty.metricsstream.HystrixMetricsStreamHandler;
import com.netflix.loadbalancer.reactive.ExecutionContext;
import com.netflix.loadbalancer.reactive.ExecutionInfo;
import com.netflix.loadbalancer.reactive.ExecutionListener;
import com.netflix.ribbon.ClientOptions;
import com.netflix.ribbon.RibbonRequest;
import com.netflix.ribbon.RibbonTransportFactory;
import com.netflix.ribbon.examples.rx.RxMovieServer;
import com.netflix.ribbon.examples.rx.common.RecommendationServiceResponseValidator;
import com.netflix.ribbon.examples.rx.template.RxMovieTemplateExample;
import com.netflix.ribbon.http.HttpRequestTemplate;
import com.netflix.ribbon.http.HttpResourceGroup;
import com.netflix.ribbon.transport.netty.RibbonTransport;
import com.netflix.ribbon.transport.netty.http.LoadBalancingHttpClient;
import com.netflix.ribbon.transport.netty.http.NettyHttpLoadBalancerErrorHandler;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.server.HttpServer;
import rx.Observable;

public class RxLatencyTest {

  private static final int HYSTRIX_METRICS_PORT = 8081;
  private static final int WORKER_TREADS = 4;
  private static final long DELAY_MS = 1000;
  private static final ExecutorService workerThreads = Executors.newFixedThreadPool(WORKER_TREADS);

  public static HttpServer<ByteBuf, ByteBuf> createHystrixMetricsServer() {
    HttpServer<ByteBuf, ByteBuf> server = RxNetty.newHttpServerBuilder(HYSTRIX_METRICS_PORT,
        new HystrixMetricsStreamHandler<ByteBuf, ByteBuf>(
          (request, response) -> Observable.empty()))
        .build();

    return server;
  }

  public static void runExample() {
    RxMovieTemplateExample example = new RxMovieTemplateExample(RxMovieServer.DEFAULT_PORT);
    example.runExample();

    HttpResourceGroup httpResourceGroup = HttpResourceGroup.Builder
        .newBuilder("testMovieServiceClient", ClientConfigFactory.DEFAULT, MyTransportFactory.INSTANCE)
        .withClientOptions(ClientOptions.create()
            .withMaxAutoRetriesNextServer(3)
            .withConfigurationBasedServerList("localhost:" + RxMovieServer.DEFAULT_PORT))
        .build();

    /*
    HttpResourceGroup httpResourceGroup = Ribbon.createHttpResourceGroup("movieServiceClient",
        ClientOptions.create()
            .withMaxAutoRetriesNextServer(3)
            .withConfigurationBasedServerList("localhost:" + RxMovieServer.DEFAULT_PORT));
            */

    HttpRequestTemplate<ByteBuf> template = httpResourceGroup.newTemplateBuilder("testRecommendationsByUserId")
        .withMethod("GET")
        .withUriTemplate("/users/{userId}/recommendations")
        .withHeader("X-Platform-Version", "xyz")
        .withHeader("X-Auth-Token", "abc")
        .withResponseValidator(new RecommendationServiceResponseValidator())
        .build();

    for (int i = 0; i < WORKER_TREADS; ++i) {
      workerThreads.submit(() -> {
        while (true) {
          try {
            Thread.sleep(DELAY_MS);
          } catch (InterruptedException e) {
            return;
          }

          try {
            RibbonRequest<ByteBuf> request = template.requestBuilder().withRequestProperty("userId", "user1").build();
            //fireAndForgetObserve(request);
            observe(request);
          } catch (Exception e) {
            System.out.println(e.toString());
            throw new RuntimeException(e);
          }
        }
      });
    }
  }

  private static void fireAndForgetObserve(RibbonRequest<ByteBuf> request) throws Exception {
    ByteBuf buf =
        request
            .observe()
            .doOnNext((b)
                -> System.out.println("refCount: " + b.refCnt()))
            .toBlocking()
            .toFuture()
            .get();

    System.out.println(buf.toString(Charset.defaultCharset()));
  }

  private static void observe(RibbonRequest<ByteBuf> request) throws Exception {
      long start = System.currentTimeMillis();
      request
          .toObservable()
          .map((b) ->
            b.toString(Charset.defaultCharset())
          )
          .subscribe((s) -> {
            long diff = System.currentTimeMillis() - start;
            if (diff > 500) {
              System.out.println("Long wait: " + diff);
            }
          });
  }

  public static void main(String[] args) {
    runExample();
    createHystrixMetricsServer().startAndWait();
  }

  private static class MyTransportFactory extends RibbonTransportFactory.DefaultRibbonTransportFactory {

    public static final MyTransportFactory INSTANCE = new MyTransportFactory();

    public MyTransportFactory() {
      super(ClientConfigFactory.DEFAULT);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> newHttpClient(IClientConfig config) {
      List<ExecutionListener<HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>>> listeners = new ArrayList<>();
      listeners.add(new TimerListener<>());

      return LoadBalancingHttpClient.<ByteBuf, ByteBuf>builder()
          .withClientConfig(config)
          .withRetryHandler(new NettyHttpLoadBalancerErrorHandler(config))
          .withPipelineConfigurator(RibbonTransport.DEFAULT_HTTP_PIPELINE_CONFIGURATOR)
          .withPoolCleanerScheduler(RibbonTransport.poolCleanerScheduler)
          .withExecutorListeners(listeners)
          .build();
    }
  }

  private static class TimerListener<I, O> implements ExecutionListener<I, O> {

    @Override
    public void onExecutionStart(ExecutionContext<I> context) throws AbortExecutionException {
      context.put("startTime", System.currentTimeMillis());
    }

    @Override
    public void onExecutionSuccess(ExecutionContext<I> context, O response, ExecutionInfo info) {
      long startTime = (long) context.get("startTime");
      long now = System.currentTimeMillis();
      long diff = now - startTime;
      if (diff > 500) {
        long serverStartTime = (long) context.get("serverStartTime");
        System.out.println("High Latency: " + diff + "ms Since server start: " + (now - serverStartTime) + "ms");
      }
    }

    @Override
    public void onStartWithServer(ExecutionContext<I> context, ExecutionInfo info)
        throws AbortExecutionException {
      context.put("serverStartTime", System.currentTimeMillis());
    }

    @Override
    public void onExceptionWithServer(ExecutionContext<I> context, Throwable exception, ExecutionInfo info) {

    }

    @Override
    public void onExecutionFailed(ExecutionContext<I> context, Throwable finalException, ExecutionInfo info) {

    }
  }

}
