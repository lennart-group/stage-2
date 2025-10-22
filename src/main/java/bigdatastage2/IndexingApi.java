package bigdatastage2;

import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexApi {

  // ---------- configuration / state ----------
  private static final Gson gson = new Gson();

  private static final Path CONTROL_DIR = Paths.get("control");
  private static final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");

  private static MongoCollection<Document> booksCollection;
  private static MongoDatabase indexDb;
  private static boolean TEST_MODE = false;

  // mock data structures for TEST_MODE
  private static final Map<String, Set<Integer>> mockInvertedIndex = new HashMap<>();
  private static final Map<Integer, String> mockBooks = new HashMap<>();
  private static final Set<Integer> mockIndexed = new HashSet<>();

  public static void main(String[] args) {
    try {
      MongoDatabase[] dbs = RepositoryConnection.connectToDB();
      TEST_MODE = (dbs == null);

      ensureControlDir();

      if (TEST_MODE) {
        System.out.println("⚙️ Running IndexApi in TEST_MODE (no DB)");
        seedMockData();
      } else {
        booksCollection = dbs[0].getCollection("books");
        indexDb = dbs[1];
        System.out.println("✅ IndexApi DB initialized");
      }
    } catch (Exception e) {
      TEST_MODE = true;
      System.out.println("⚠️ Falling back to TEST_MODE: " + e.getMessage());
      seedMockData();
    }

    // Javalin app setup
    Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
        .start(7004);

    // endpoints
    app.get("/status", IndexApi::status);
    app.post("/index/book/:id", IndexApi::indexSingle);
    app.post("/index/all", IndexApi::indexAll);
    app.post("/index/reset", IndexApi::resetIndexedFlag);
    app.get("/index/summary", IndexApi::summary);

    System.out.println("🚀 Index API running on port 7004 (" + (TEST_MODE ? "TEST_MODE" : "LIVE") + ")");
  }

  // ---------- handlers ----------

  private static void status(Context ctx) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("service", "index-service");
    m.put("mode", TEST_MODE ? "TEST_MODE" : "LIVE");
    m.put("control_file", INDEXED_FILE.toString());
    m.put("database", TEST_MODE ? "not-connected" : "connected");
    ctx.result(gson.toJson(m));
  }

  private static void indexSingle(Context ctx) {
    String idStr = ctx.pathParam("id");
    try {
      int id = Integer.parseInt(idStr);

      Optional<String> textOpt = fetchBookText(id);
      if (textOpt.isEmpty()) {
        ctx.status(404).result(gson.toJson(Map.of(
            "error", "Book not found: " + id
        )));
        return;
      }

      int unique = processBook(id, textOpt.get());

      ctx.result(gson.toJson(Map.of(
          "indexed", true,
          "book_id", id,
          "unique_terms", unique
      )));
    } catch (NumberFormatException nfe) {
      ctx.status(400).result(gson.toJson(Map.of("error", "Invalid book id")));
    } catch (Exception e) {
      e.printStackTrace();
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  private static void indexAll(Context ctx) {
    int count = 0;
    int termsTotal = 0;
    try {
      if (TEST_MODE) {
        for (Map.Entry<Integer, String> e : mockBooks.entrySet()) {
          int id = e.getKey();
          if (alreadyIndexed(id)) continue;
          termsTotal += processBook(id, e.getValue());
          count++;
        }
      } else {
        try (MongoCursor<Document> cursor = booksCollection.find().iterator()) {
          while (cursor.hasNext()) {
            Document d = cursor.next();
            Integer id = d.getInteger("id");
            if (id == null) continue;
            if (alreadyIndexed(id)) continue;

            String text = d.getString("content");
            if (text == null) text = "";

            termsTotal += processBook(id, text);
            count++;
          }
        }
      }

      ctx.result(gson.toJson(Map.of(
          "indexed_books", count,
          "total_unique_terms_added", termsTotal
      )));
    } catch (Exception e) {
      e.printStackTrace();
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  private static void resetIndexedFlag(Context ctx) {
    try {
      if (TEST_MODE) {
        mockIndexed.clear();
        mockInvertedIndex.clear();
      }
      if (Files.exists(INDEXED_FILE)) {
        Files.delete(INDEXED_FILE);
      }
      ctx.result(gson.toJson(Map.of("reset", true)));
    } catch (IOException e) {
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  private static void summary(Context ctx) {
    Map<String, Object> m = new LinkedHashMap<>();
    try {
      m.put("mode", TEST_MODE ? "TEST_MODE" : "LIVE");
      m.put("indexed_count", TEST_MODE ? mockIndexed.size() : countIndexedFromFile());
      if (TEST_MODE) {
        m.put("mock_terms", mockInvertedIndex.size());
      }
      ctx.result(gson.toJson(m));
    } catch (Exception e) {
      ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
    }
  }

  // ---------- core indexing ----------

  private static int processBook(int bookId, String text) throws Exception {
    Set<String> terms = tokenize(text);

    if (TEST_MODE) {
      for (String t : terms) {
        mockInvertedIndex.computeIfAbsent(t, k -> new HashSet<>()).add(bookId);
      }
    } else {
      for (String t : terms) {
        updateMongoInvertedIndex(t, bookId);
      }
    }

    markIndexed(bookId);
    System.out.printf("✅ Indexed book %d (%d unique terms).%n", bookId, terms.size());
    return terms.size();
  }

  private static Set<String> tokenize(String text) {
    Set<String> tokens = new HashSet<>();
    if (text == null) return tokens;
    Matcher m = Pattern.compile("\\b[a-z]{2,}\\b").matcher(text.toLowerCase());
    while (m.find()) tokens.add(m.group());
    return tokens;
  }

  private static void updateMongoInvertedIndex(String term, int bookId) {
    String bucket = term.substring(0, 1);
    MongoCollection<Document> col = indexDb.getCollection(bucket);
    col.updateOne(
        Filters.eq("term", term),
        Updates.addToSet("postings", bookId),
        new UpdateOptions().upsert(true)
    );
  }

  // ---------- IO helpers ----------

  private static void ensureControlDir() throws IOException {
    if (!Files.exists(CONTROL_DIR)) Files.createDirectories(CONTROL_DIR);
  }

  private static void markIndexed(int bookId) throws IOException {
    if (TEST_MODE) {
      mockIndexed.add(bookId);
    }
    ensureControlDir();
    try (BufferedWriter bw = Files.newBufferedWriter(
        INDEXED_FILE, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      bw.write(String.valueOf(bookId));
      bw.newLine();
    }
  }

  private static boolean alreadyIndexed(int bookId) {
    if (TEST_MODE) return mockIndexed.contains(bookId);
    if (!Files.exists(INDEXED_FILE)) return false;
    try {
      for (String line : Files.readAllLines(INDEXED_FILE, StandardCharsets.UTF_8)) {
        if (line.trim().equals(String.valueOf(bookId))) return true;
      }
    } catch (IOException ignore) {}
    return false;
  }

  private static int countIndexedFromFile() {
    if (!Files.exists(INDEXED_FILE)) return 0;
    try {
      return (int) Files.lines(INDEXED_FILE, StandardCharsets.UTF_8).filter(s -> !s.isBlank()).count();
    } catch (IOException e) {
      return 0;
    }
  }

  private static Optional<String> fetchBookText(int id) {
    if (TEST_MODE) {
      return Optional.ofNullable(mockBooks.get(id));
    }
    Document doc = booksCollection.find(Filters.eq("id", id)).first();
    if (doc == null) return Optional.empty();
    return Optional.ofNullable(doc.getString("content"));
  }

  private static void seedMockData() {
    mockBooks.put(1, "Alice was beginning to get very tired of sitting by her sister on the bank.");
    mockBooks.put(2, "It was the best of times, it was the worst of times.");
    mockBooks.put(3, "Call me Ishmael. Some years ago—never mind how long precisely.");
  }
}
