package bigdatastage2;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kleine REST-Schicht zum Auslösen der Orchestrierung.
 *
 * Endpoints:
 *  GET  /control/status
 *  POST /control/run/{book_id}
 *  POST /control/run-batch     (Body: {"book_ids":[1,2,3]})
 *  GET  /control/processed     (Liste bereits verarbeiteter IDs)
 */
public class ControlApi {

  private static final Gson gson = new Gson();

  public static void main(String[] args) {
    String portStr       = env("CONTROL_PORT", "7000");
    String ingestionBase = env("INGESTION_BASE", "http://ingestion:7001");
    String indexingBase  = env("INDEXING_BASE",  "http://indexing:7002");
    String searchBase    = env("SEARCH_BASE",    "http://search:7003");
    String processedPath = env("PROCESSED_LIST", "control/indexed_books.txt");

    ControlModule control = new ControlModule(ingestionBase, indexingBase, searchBase, Path.of(processedPath));

    Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
        .start(Integer.parseInt(portStr));

    app.get("/control/status", ctx -> {
      Map<String, Object> status = new LinkedHashMap<>();
      status.put("service", "control");
      status.put("status", "running");
      status.put("ingestionBase", ingestionBase);
      status.put("indexingBase", indexingBase);
      status.put("searchBase", searchBase);
      ctx.result(gson.toJson(status));
    });

    app.post("/control/run/{book_id}", ctx -> {
      int bookId = Integer.parseInt(ctx.pathParam("book_id"));
      try {
        var res = control.ingestAndIndex(bookId);
        ctx.result(gson.toJson(res));
      } catch (Exception e) {
        ctx.status(500).result(gson.toJson(Map.of(
            "book_id", bookId, "status", "error", "message", e.getMessage()
        )));
      }
    });

    app.post("/control/run-batch", ctx -> {
      try {
        Map<?,?> req = gson.fromJson(ctx.body(), Map.class);
        List<?> raw = (List<?>) req.getOrDefault("book_ids", List.of());
        List<Integer> ids = raw.stream()
            .map(o -> (o instanceof Number) ? ((Number)o).intValue() : Integer.parseInt(o.toString()))
            .collect(Collectors.toList());
        var res = control.ingestAndIndexBatch(ids);
        ctx.result(gson.toJson(Map.of("count", res.size(), "results", res)));
      } catch (Exception e) {
        ctx.status(400).result(gson.toJson(Map.of(
            "status", "error", "message", "invalid body, expected {\"book_ids\":[...]}: " + e.getMessage()
        )));
      }
    });

    app.get("/control/processed", ctx -> {
      // nicht perfekt effizient, aber praktisch: Datei zurückgeben, wenn vorhanden
      try {
        var p = Path.of(processedPath);
        if (!java.nio.file.Files.exists(p)) {
          ctx.result(gson.toJson(Map.of("processed", List.of())));
          return;
        }
        var list = java.nio.file.Files.readAllLines(p).stream()
            .map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList());
        ctx.result(gson.toJson(Map.of("processed", list)));
      } catch (Exception e) {
        ctx.status(500).result(gson.toJson(Map.of("status", "error", "message", e.getMessage())));
      }
    });
  }

  private static String env(String k, String d) {
    return System.getenv().getOrDefault(k, d);
  }
}
