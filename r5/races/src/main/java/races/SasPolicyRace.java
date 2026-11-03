package races;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.azure.adlsv2.VendedAdlsCredentialProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * C-54. A fresh real VendedAzureSasCredentialPolicy (package-private, built by reflection) per
 * trial, installed in a real azure-core HttpPipeline with a stub transport. The SAS token comes
 * from the provider's properties (adls.sas-token.<account>), so no HTTP to a catalog is made. W
 * threads send their first request at the same time. A failure is attributed structurally: an
 * NPE whose top frame is VendedAzureSasCredentialPolicy.processSync (the unguarded read of the
 * plain azureSasCredentialPolicy field). A success must carry the SAS token in the URL query.
 *
 * <p>args: mode(concurrent|sequential) trials writers
 */
public class SasPolicyRace {
  static final String ACCOUNT = "acct.dfs.core.windows.net";
  static final String TOKEN = "sv=2024-01-01&sig=r5test";

  static class StubResponse extends HttpResponse {
    StubResponse(HttpRequest r) {
      super(r);
    }

    @Override
    public int getStatusCode() {
      return 200;
    }

    @Override
    @SuppressWarnings("deprecation")
    public String getHeaderValue(String name) {
      return null;
    }

    @Override
    public HttpHeaders getHeaders() {
      return new HttpHeaders();
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return Flux.empty();
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(new byte[0]);
    }

    @Override
    public Mono<String> getBodyAsString() {
      return Mono.just("");
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just("");
    }
  }

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    if (!mode.equals("concurrent") && !mode.equals("sequential")) {
      throw new IllegalArgumentException("unknown mode " + mode);
    }
    int trials = Integer.parseInt(args[1]);
    int writers = Integer.parseInt(args[2]);
    String providerMode = args[3];
    if (!providerMode.equals("fresh") && !providerMode.equals("shared")) {
      throw new IllegalArgumentException("unknown provider mode " + providerMode);
    }
    Class<?> cls = Class.forName("org.apache.iceberg.azure.adlsv2.VendedAzureSasCredentialPolicy");
    Constructor<?> ctor = cls.getDeclaredConstructor(String.class, VendedAdlsCredentialProvider.class);
    ctor.setAccessible(true);
    HttpClient stub =
        new HttpClient() {
          @Override
          public Mono<HttpResponse> send(HttpRequest request) {
            return Mono.just(new StubResponse(request));
          }

          @Override
          public HttpResponse sendSync(HttpRequest request, Context context) {
            return new StubResponse(request);
          }
        };
    Map<String, String> props =
        Map.of(
            "credentials.uri", "http://localhost:1/unused",
            "uri", "http://localhost:1/unused",
            "adls.sas-token." + ACCOUNT, TOKEN,
            "adls.sas-token-expires-at-ms." + ACCOUNT,
            String.valueOf(System.currentTimeMillis() + 3_600_000L));

    AtomicInteger npeAtField = new AtomicInteger();
    AtomicInteger missingToken = new AtomicInteger();
    AtomicInteger ok = new AtomicInteger();
    int badTrials = 0;
    Map<String, Integer> other = new TreeMap<>();
    // shared: one provider per FileIO with its token already cached, a new policy per new client,
    // which is how ADLSFileIO.buildClient uses them
    VendedAdlsCredentialProvider sharedProvider = new VendedAdlsCredentialProvider(props);
    {
      HttpPipelinePolicy warm = (HttpPipelinePolicy) ctor.newInstance(ACCOUNT, sharedProvider);
      new HttpPipelineBuilder().policies(warm).httpClient(stub).build()
          .sendSync(new HttpRequest(HttpMethod.GET, "https://" + ACCOUNT + "/c/f"), Context.NONE);
    }
    for (int trial = 0; trial < trials; trial++) {
      VendedAdlsCredentialProvider provider =
          providerMode.equals("shared") ? sharedProvider : new VendedAdlsCredentialProvider(props);
      HttpPipelinePolicy policy = (HttpPipelinePolicy) ctor.newInstance(ACCOUNT, provider);
      HttpPipeline pipeline = new HttpPipelineBuilder().policies(policy).httpClient(stub).build();
      CyclicBarrier barrier = new CyclicBarrier(mode.equals("concurrent") ? writers : 1);
      AtomicInteger bad = new AtomicInteger();
      Runnable body =
          () -> {
            try {
              barrier.await();
              HttpRequest req =
                  new HttpRequest(HttpMethod.GET, "https://" + ACCOUNT + "/container/file");
              HttpResponse resp = pipeline.sendSync(req, Context.NONE);
              String q = resp.getRequest().getUrl().getQuery();
              if (q == null || !q.contains("sig=r5test")) {
                missingToken.incrementAndGet();
                bad.incrementAndGet();
              } else {
                ok.incrementAndGet();
              }
            } catch (NullPointerException e) {
              StackTraceElement top = e.getStackTrace().length > 0 ? e.getStackTrace()[0] : null;
              // the only dereference in processSync itself is the plain azureSasCredentialPolicy
              // field (VendedAzureSasCredentialPolicy.java:57)
              if (top != null && top.getClassName().equals(cls.getName())
                  && top.getMethodName().equals("processSync")) {
                npeAtField.incrementAndGet();
              } else {
                synchronized (other) {
                  other.merge("NPE@" + top, 1, Integer::sum);
                }
              }
              bad.incrementAndGet();
            } catch (Throwable e) {
              synchronized (other) {
                other.merge(e.getClass().getName(), 1, Integer::sum);
              }
              bad.incrementAndGet();
            }
          };
      if (mode.equals("concurrent")) {
        Thread[] ts = new Thread[writers];
        for (int w = 0; w < writers; w++) {
          ts[w] = new Thread(body);
          ts[w].start();
        }
        for (Thread t : ts) {
          t.join();
        }
      } else {
        for (int w = 0; w < writers; w++) {
          body.run();
        }
      }
      if (bad.get() > 0) {
        badTrials++;
      }
      if (provider != sharedProvider) {
        provider.close();
      }
    }
    System.out.printf(
        "RESULT version=%s provider=" + providerMode + " mode=%s trials=%d writers=%d badTrials=%d ok=%d npeAtUnpublishedField=%d "
            + "missingToken=%d other=%s%n",
        System.getProperty("icebergVersion"), mode, trials, writers, badTrials, ok.get(),
        npeAtField.get(), missingToken.get(), other);
    System.exit(0);
  }
}
