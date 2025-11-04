package bigdatastage2;

import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexingAPI {

  // ---------- configuration / state ----------
  private static final Gson gson = new Gson();

  private static final Path CONTROL_DIR = Paths.get("control");
  private static final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");
  private static final int PORT = 7004;

  private static MongoCollection<Document> booksCollection;
  private static MongoDatabase indexDb;
  private static LocalDateTime lastUpdate = null;

  public static void main(String[] args) {
    try {
      MongoDatabase[] dbs = RepositoryConnection.connectToDB();

      ensureControlDir();

      booksCollection = dbs[0].getCollection("books");
      indexDb = dbs[1];
      System.out.println("✅ IndexApi DB initialized");
    } catch (Exception e) {
      System.err.println("An error occured while connecting to the database" + e.getMessage());
      return;
    }

    // Javalin app setup
    Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
        .start(PORT);

    // endpoints
    app.get("/status", IndexingAPI::status);
    app.post("/index/update/{book_id}", IndexingAPI::indexSingle);
    app.post("/index/rebuild", IndexingAPI::indexAll);
    app.get("/index/status", IndexingAPI::indexStatus);

    System.out.println("🚀 Index API running on port: " + PORT);
  }

  // ---------- handlers ----------

  private static void status(Context ctx) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("service", "index-service");
    m.put("control_file", INDEXED_FILE.toString());
    m.put("database", "connected");
    ctx.result(gson.toJson(m));
  }

  private static void indexSingle(Context ctx) {
    String idStr = ctx.pathParam("book_id");
    try {
      int id = Integer.parseInt(idStr);
      if (alreadyIndexed(id)) {
        System.out.printf("Book %d already indexed, returning.\n", id);
        ctx.result(gson.toJson(Map.of(
            "book_id", id,
            "index", "already up to date")));
        return;
      }

      String text = booksCollection.find(Filters.eq("id", id))
          .first()
          .getString("content");

      if (text == null) {
        ctx.status(404).result(gson.toJson(Map.of(
            "error", "Book not found: " + id)));
        return;
      }

      processBook(id, text);

      lastUpdate = LocalDateTime.now();

      ctx.result(gson.toJson(Map.of(
          "book_id", id,
          "index", "updated")));
    } catch (NumberFormatException nfe) {
      ctx.status(400).result("Invalid book_id: must be a number");
    } catch (Exception e) {
      e.printStackTrace();
      ctx.status(500).result(gson.toJson(Map.of("error", String.valueOf(e.getMessage()))));
    }
  }

  private static void indexAll(Context ctx) {
    try {
      // 1️⃣ Lade alle Bücher zuerst in eine Liste (Cursor wird schnell geschlossen)
      List<Document> books = booksCollection.find()
          .projection(Projections.include("id", "content")) // nur benötigte Felder
          .into(new ArrayList<>());

      System.out.println("Rebuilding the index for " + books.size() + " books.");

      for (Document collection : indexDb.listCollections()) {
        collection.clear();
      }

      INDEXED_FILE.toFile().delete();

      // Filter Bücher ohne ID oder bereits indexierte Bücher
      List<Document> toIndex = books.stream()
          .filter(d -> {
            Integer id = d.getInteger("id");
            if (id == null)
              return false;
            // if (alreadyIndexed(id)) {
            //   System.out.printf("Book %d already indexed, skipping.%n", id);
            //   return false;
            // }
            return true;
          })
          .toList();

      // 2️⃣ Parallel-Verarbeitung mit ExecutorService
      ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      AtomicInteger booksProcessed = new AtomicInteger();
      AtomicInteger termsTotal = new AtomicInteger();

      List<Future<?>> futures = new ArrayList<>();
      for (Document d : toIndex) {
        futures.add(executor.submit(() -> {
          try {
            int id = d.getInteger("id");
            String text = d.getString("content");
            if (text == null)
              return; // skip books without content

            // Prozess Buch inkl. bulk MongoDB update
            Set<String> terms = tokenize(text);
            updateMongoInvertedIndexBulk(terms, id);

            markIndexed(id);
            booksProcessed.incrementAndGet();
            termsTotal.addAndGet(terms.size());

            System.out.printf("✅ Indexed book %d (%d unique terms).%n", id, terms.size());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }));
      }

      // 3️⃣ Warten bis alle Tasks fertig sind
      for (Future<?> f : futures) {
        f.get();
      }

      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.HOURS);

      lastUpdate = LocalDateTime.now();

      // 4️⃣ Ergebnis an den Client
      ctx.result(gson.toJson(Map.of(
          "books_processed", booksProcessed.get(),
          "terms_indexed", termsTotal.get())));
    } catch (Exception e) {
      e.printStackTrace();
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  private static void indexStatus(Context ctx) {
    Map<String, Object> m = new LinkedHashMap<>();
    try {
      m.put("books_indexed", countIndexedFromFile());
      m.put("last_update", lastUpdate != null ? lastUpdate.toString() : "unknown");
      Document stats = indexDb.runCommand(new Document("dbStats", 1));
      double sizeInMB = stats.getLong("dataSize") / (1024 * 1024);
      m.put("index_size_MB", sizeInMB);
      ctx.result(gson.toJson(m));
    } catch (Exception e) {
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  // ---------- core indexing ----------

  private static void processBook(int bookId, String text) throws Exception {
    Set<String> terms = tokenize(text);

    // Bulk update MongoDB inverted index
    updateMongoInvertedIndexBulk(terms, bookId);

    markIndexed(bookId);
    System.out.printf("✅ Indexed book %d (%d unique terms).%n", bookId, terms.size());
  }

  private static Set<String> tokenize(String text) {
    Set<String> tokens = new HashSet<>();
    if (text == null)
      return tokens;
    Matcher m = Pattern.compile("\\b[a-z]{2,}\\b").matcher(text.toLowerCase());
    while (m.find())
      tokens.add(m.group());
    return tokens;
  }

  private static void updateMongoInvertedIndexBulk(Set<String> terms, int bookId) {
    // Map bucket -> List of term updates
    Map<String, List<WriteModel<Document>>> bucketWrites = new HashMap<>();

    for (String term : terms) {
      String bucket = term.substring(0, 1);
      bucketWrites.computeIfAbsent(bucket, k -> new ArrayList<>())
          .add(new UpdateOneModel<>(
              Filters.eq("term", term),
              Updates.addToSet("postings", bookId),
              new UpdateOptions().upsert(true)));
    }

    // Write each bucket in bulk
    for (Map.Entry<String, List<WriteModel<Document>>> entry : bucketWrites.entrySet()) {
      String bucket = entry.getKey();
      List<WriteModel<Document>> writes = entry.getValue();
      if (!writes.isEmpty()) {
        MongoCollection<Document> col = indexDb.getCollection(bucket);
        col.bulkWrite(writes, new BulkWriteOptions().ordered(false));
      }
    }
  }

  // ---------- IO helpers ----------

  private static void ensureControlDir() throws IOException {
    if (!Files.exists(CONTROL_DIR))
      Files.createDirectories(CONTROL_DIR);
  }

  private static void markIndexed(int bookId) throws IOException {
    ensureControlDir();
    try (BufferedWriter bw = Files.newBufferedWriter(
        INDEXED_FILE, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      bw.write(String.valueOf(bookId));
      bw.newLine();
    }
  }

  private static boolean alreadyIndexed(int bookId) {
    if (!Files.exists(INDEXED_FILE))
      return false;
    try {
      for (String line : Files.readAllLines(INDEXED_FILE, StandardCharsets.UTF_8)) {
        if (line.trim().equals(String.valueOf(bookId)))
          return true;
      }
    } catch (IOException ignore) {
    }
    return false;
  }

  private static int countIndexedFromFile() {
    if (!Files.exists(INDEXED_FILE))
      return 0;
    try {
      return (int) Files.lines(INDEXED_FILE, StandardCharsets.UTF_8).filter(s -> !s.isBlank()).count();
    } catch (IOException e) {
      return 0;
    }
  }
}
