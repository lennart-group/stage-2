package bigdatastage2;

import org.openjdk.jmh.annotations.*;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmarks:
 *  - Indexing throughput (books/sec)
 *  - Query latency and concurrency behavior
 *  - CPU and memory utilization per container
 *  - Scalability limits on a 4-core / 32 GB reference server
 *
 * Run with:
 *   mvn clean package
 *   java -Xmx4G -jar target/Benchmarking.jar -rf json -rff benchmarking_results.json
 *
 * Results can be visualized in JMH Visualizer (https://jmh.morethan.io)
 * or plotted manually (CSV import).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)          // >= 5 warmups for JVM stabilization
@Measurement(iterations = 10)    // >= 10 measurement iterations
@Fork(1)
@State(Scope.Benchmark)
public class Benchmarking {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    Dotenv dotenv = Dotenv.load();
    String ingestionBase = dotenv.get("INGESTING_API");
    String indexingBase = dotenv.get("INDEXING_API");
    String searchBase = dotenv.get("SEARCH_API");

    @Param({ "50", "100", "150" })
    int bookNumber;

    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    private final Runtime runtime = Runtime.getRuntime();

    // -------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------
    @Setup(Level.Trial)
    public void setup() {
        System.out.printf("Running JMH benchmarks for book %d%n", bookNumber);
    }

    // -------------------------------------------------------------
    // Core Endpoints
    // -------------------------------------------------------------
    @Benchmark
    public void getIngestionStatus() {
        sendGet(ingestionBase + "/ingest/status/" + bookNumber, 10);
    }

    @Benchmark
    public void ingestBook() {
        sendPost(ingestionBase + "/ingest/" + bookNumber, null, 30);
    }

    @Benchmark
    public void getIngestingList() {
        sendGet(ingestionBase + "/ingest/list", 10);
    }

    @Benchmark
    public void indexBook() {
        sendPost(indexingBase + "/index/update/" + bookNumber, null, 300);
    }

    // -------------------------------------------------------------
    // Advanced Benchmarks (Section 6)
    // -------------------------------------------------------------

    /**
     * Indexing throughput — books processed per second
     */
    @Benchmark
    public double indexingThroughput() {
        Instant start = Instant.now();
        int processed = 0;

        for (int i = 0; i < 5; i++) {
            sendPost(indexingBase + "/index/update/" + (bookNumber + i), null, 120);
            processed++;
        }

        double durationSec = Duration.between(start, Instant.now()).toMillis() / 1000.0;
        return processed / durationSec; // throughput: books/sec
    }

    /**
     * Query latency under concurrent load
     */
    @Benchmark
    @Threads(8)
    public double concurrentQueryLatency() {
        Instant start = Instant.now();
        sendGet(searchBase + "/search?q=benchmark", 10);
        return Duration.between(start, Instant.now()).toMillis(); // latency in ms
    }

    /**
     * CPU utilization after ingesting
     */
    @Benchmark
    public double ingestingResourceUtilization_CPU() {
        sendPost(ingestionBase + "/ingest/" + bookNumber, null, 200);
        double load = osBean.getSystemLoadAverage();
        return load >= 0 ? load : 0.0; // load average per CPU
    }

    /**
     * Memory utilization after ingesting (MB)
     */
    @Benchmark
    public double ingestingResourceUtilization_MEM() {
        sendPost(ingestionBase + "/ingest/" + bookNumber, null, 200);
        long usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        return usedMem; // MB
    }

    /**
     * CPU utilization after indexing
     */
    @Benchmark
    public double indexingResourceUtilization_CPU() {
        sendPost(indexingBase + "/index/update/" + bookNumber, null, 200);
        double load = osBean.getSystemLoadAverage();
        return load >= 0 ? load : 0.0; // load average per CPU
    }

    /**
     * Memory utilization after indexing (MB)
     */
    @Benchmark
    public double indexingResourceUtilization_MEM() {
        sendPost(indexingBase + "/index/update/" + bookNumber, null, 200);
        long usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        return usedMem; // MB
    }

    /**
     * CPU utilization after indexing
     */
    @Benchmark
    public double searchResourceUtilization_CPU() {
        sendPost(indexingBase + "/search?q=different" + bookNumber, null, 200);
        double load = osBean.getSystemLoadAverage();
        return load >= 0 ? load : 0.0; // load average per CPU
    }

    /**
     * Memory utilization after indexing (MB)
     */
    @Benchmark
    public double searchResourceUtilization_MEM() {
        sendPost(indexingBase + "/search?q=different" + bookNumber, null, 200);
        long usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        return usedMem; // MB
    }

    /**
     * Scalability test on 4-core reference server (simulated)
     */
    @Benchmark
    public double scalabilityTest() {
        int threads = 4; // mimic reference hardware
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger completed = new AtomicInteger(0);
        Instant start = Instant.now();

        for (int i = 0; i < threads; i++) {
            exec.submit(() -> {
                sendPost(indexingBase + "/index/update/" + (bookNumber + completed.incrementAndGet()), null, 300);
            });
        }

        exec.shutdown();
        try {
            exec.awaitTermination(600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        double durationSec = Duration.between(start, Instant.now()).toMillis() / 1000.0;
        return durationSec; // time (s) — used to infer scalability
    }

    // -------------------------------------------------------------
    // HTTP Utilities
    // -------------------------------------------------------------
    private HttpResponse<String> sendPost(String url, String body, int timeoutSec) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSec))
                    .POST(body == null ? HttpRequest.BodyPublishers.noBody()
                            : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("HTTP POST failed for: " + url, e);
        }
    }

    private HttpResponse<String> sendGet(String url, int timeoutSec) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSec))
                    .GET().build();
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("HTTP GET failed for: " + url, e);
        }
    }

    // -------------------------------------------------------------
    // Notes for Report
    // -------------------------------------------------------------
    /**
     * Visualizations (Section 6):
     *   • Table: Indexing throughput (books/sec)
     *   • Chart: Query latency vs concurrency
     *   • Graph: CPU and Memory utilization over time
     *   • Trend: Scalability efficiency on 4 cores
     *
     * Discussion:
     *   - Identify I/O or CPU bottlenecks from throughput curves.
     *   - Observe latency increase under load (8 threads).
     *   - Report when throughput plateaus to infer scaling limit.
     */
}
