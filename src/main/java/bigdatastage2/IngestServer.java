package bigdatastage2;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.util.JavalinBindException;

import com.google.gson.Gson;
import java.util.*;

public class IngestServer {

  private static final Gson gson = new Gson();

  public static void main(String[] args) {
    Javalin app = Javalin.create(config -> {
      config.http.defaultContentType = "application/json";
    }).start(7000);

    app.get("/status", ctx -> {
      Map<String, Object> status = Map.of(
          "service", "example-service",
          "status", "running");
      ctx.result(gson.toJson(status));
    });

    // POST /ingest/{book_id}
    app.post("/ingest/{book_id}", IngestServer::handleIngestBook);

    // GET /ingest/status/{book_id}
    app.get("/ingest/status/{book_id}", IngestServer::handleGetStatus);

    // GET /ingest/list
    app.get("/ingest/list", IngestServer::handleListBooks);
  }

  public static void handleIngestBook(Context ctx) {

  }

  public static void handleGetStatus(Context ctx) {

  }
  
  public static void handleListBooks(Context ctx) {

  }
}
