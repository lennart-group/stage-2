package bigdatastage2;

import io.javalin.Javalin;
import io.javalin.http.Context;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import org.bson.Document;

public class IngestServer {

  private static final Gson gson = new Gson();
  private static MongoDatabase[] databases;
  private static MongoCollection<Document> booksCollection;
  private static final int PORT = 7000;

  public static void main(String[] args) {

    try {
      databases = RepositoryConnection.connectToDB();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    booksCollection = databases[0].getCollection("books");

    Javalin app = Javalin.create(config -> {
      config.http.defaultContentType = "application/json";
    }).start(PORT);
    System.out.println("Running on port:" + PORT);

    // Health check status
    app.get("/status", ctx -> {
      Map<String, Object> status = Map.of(
          "service", "ingest-service",
          "status", "running",
          "database", "connected");
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
    String bookId = ctx.pathParam("book_id");
    int idNum;
    try {
      idNum = Integer.parseInt(bookId);
      // Buch aus DB holen
    } catch (NumberFormatException e) {
      ctx.status(400).result("Invalid book_id: must be a number");
      return;
    }
    try {
      // Download book from Project Gutenberg
      String urlString = "https://www.gutenberg.org/cache/epub/" + bookId + "/pg" + bookId + ".txt";
      String bookContent = downloadBook(urlString);

      // Strip metadata from book
      String title = extractMetadata(bookContent, "Title:");
      String author = extractMetadata(bookContent, "Author:");
      String releaseDate = extractReleaseDate(bookContent);
      String language = extractMetadata(bookContent, "Language:");

      String[] contentAndFooter = extractContent(bookContent);

      System.out.println("Inserting book with id: " + bookId + " and title: " + title + " by " + author);

      Document book = buildDbEntry(idNum, contentAndFooter[0], title, author, releaseDate, language,
          contentAndFooter[1]);
      booksCollection.replaceOne(Filters.eq("id", idNum), book, new ReplaceOptions().upsert(true));
      Map<String, Object> response = new LinkedHashMap<>();
    response.put("book_id", bookId);
    response.put("status", "downloaded");
    response.put("path", "BigData.books");
    ctx.result(gson.toJson(response));
    } catch (Exception e) {
      System.err.println(e.getMessage());
      ctx.result(e.getMessage());
    }
  }

  public static void handleGetStatus(Context ctx) {
    String bookId = ctx.pathParam("book_id");
    int idNum;
    try {
      idNum = Integer.parseInt(bookId);
      // Buch aus DB holen
    } catch (NumberFormatException e) {
      ctx.status(400).result("Invalid book_id: must be a number");
      return;
    }
    String status = "available";
    Document book = booksCollection.find(Filters.eq("id", idNum)).first();
    if (book == null) {
      status = "unavailable";
    }
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("book_id", bookId);
    response.put("status", status);
    ctx.result(gson.toJson(response));
  }

  public static void handleListBooks(Context ctx) {
    long bookCount = booksCollection.countDocuments();
    List<Integer> idList = new ArrayList<>();

    try (MongoCursor<Document> cursor = booksCollection.find().projection(Projections.include("id")).iterator()) {
      while (cursor.hasNext()) {
        Document doc = cursor.next();
        idList.add(doc.getInteger("id"));

      }
    } catch (Exception e) {
      // TODO: handle exception
    }
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", bookCount);
    response.put("books", idList);
    ctx.result(gson.toJson(response));
  }

  private static String downloadBook(String urlStr) throws IOException, InterruptedException {
    URI uri = URI.create(urlStr);
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to download book: HTTP " + response.statusCode());
    }

    return response.body();
  }

  private static String extractMetadata(String text, String key) {
    for (String line : text.split("\n")) {
      if (line.startsWith(key)) {
        return line.substring(key.length()).trim();
      }
    }
    return "";
  }

  private static String extractReleaseDate(String text) {
    String releaseDate = "";
    String[] lines = text.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("Release date:")) {
        releaseDate = lines[i].substring("Release date:".length()).trim();
        // Prüfen, ob nächste Zeile mit "Most recently updated:" beginnt
        if (i + 1 < lines.length && lines[i + 1].trim().startsWith("Most recently updated:")) {
          releaseDate += " | " + lines[i + 1].trim();
        }
        break;
      }
    }
    return releaseDate;
  }

  private static String[] extractContent(String text) {
    int start = text.indexOf("*** START OF THE PROJECT GUTENBERG EBOOK");
    int end = text.indexOf("*** END OF THE PROJECT GUTENBERG EBOOK");

    if (start != -1 && end != -1) {
      String content = text.substring(start, end).replaceAll("(?m)^\\*{3}.*$", "").trim();
      String footer = text.substring(end);
      return new String[] { content, footer };
    } else {
      return null; // fallback
    }
  }

  private static Document buildDbEntry(int book_id, String content, String title, String author, String releaseDate,
      String language, String footer) {
    return new Document()
        .append("id", book_id)
        .append("title", title)
        .append("author", author)
        .append("release_date", releaseDate)
        .append("language", language)
        .append("content", content)
        .append("footer", footer);
  }
}
