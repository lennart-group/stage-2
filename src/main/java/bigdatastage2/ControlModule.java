package bigdatastage2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;

/**
 * Orchestriert: Ingestion -> (warten) -> Indexing.
 * Hält Buch über bereits verarbeitete book_ids in indexed_books.txt.
 */
public class ControlModule {

  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private String ingestionBase;
  private String indexingBase;
  private final Path processedListFile;

  public ControlModule(String ingestionBase, String indexingBase, Path processedListFile) {
    this.ingestionBase = stripTrailingSlash(ingestionBase);
    this.indexingBase = stripTrailingSlash(indexingBase);
    this.processedListFile = processedListFile;
  }

  private static String stripTrailingSlash(String s) {
    return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  public String downloadBook(int bookId) throws Exception {
    HttpResponse<String> statusResp = sendGet(ingestionBase + "/ingest/status/" + bookId, 30);
    JsonObject json = JsonParser.parseString(statusResp.body()).getAsJsonObject();
    String status = json.get("status").getAsString();
    if (status.equals("available")) {
      return "Book is already in the database.";
    }
    HttpResponse<String> ingestResp = sendPost(ingestionBase + "/ingest/" + bookId, null, 30);
    if (ingestResp.statusCode() != 200) {
      return "Ingestion failed with status code: " + ingestResp.statusCode();
    }
    return "Ingestion completed for book ID: " + bookId;
  }

  public String indexBook(int bookId) throws Exception {
    boolean indexed = Files.lines(processedListFile).anyMatch(line -> line.equals(Integer.toString(bookId)));
    if (indexed) {
      return "Book " + bookId + " is already indexed.";
    }
    HttpResponse<String> indexResp = sendPost(indexingBase + "/index/update/" + bookId, null, 10000);
    if (indexResp.statusCode() != 200) {
      return "Indexing failed with status code: " + indexResp.statusCode();
    }
    return "Indexing completed for book ID: " + bookId;
  }

  // TODO: Search function

  // ---------- helpers ----------

  private HttpResponse<String> sendPost(String url, String body, int timeoutSec)
      throws IOException, InterruptedException {
    HttpRequest.Builder b = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(timeoutSec))
        .POST(body == null ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> sendGet(String url, int timeoutSec) throws IOException, InterruptedException {
    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(timeoutSec))
        .GET().build();
    return http.send(req, HttpResponse.BodyHandlers.ofString());
  }

  @FunctionalInterface
  private interface CheckedSupplier<T> {
    T get() throws Exception;
  }
}
