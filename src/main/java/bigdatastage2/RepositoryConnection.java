package bigdatastage2;

import io.github.cdimascio.dotenv.Dotenv;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.result.InsertManyResult;
import org.bson.Document;

import java.io.FileNotFoundException;
import java.util.*;

public class RepositoryConnection {

  public static void main(String[] args) throws FileNotFoundException {

    Dotenv dotenv = Dotenv.load();

    String user = dotenv.get("SERVICES_USER");
    String password = dotenv.get("SERVICES_PASSWORD");
    String host = dotenv.get("MONGO_HOST");
    String port = dotenv.get("MONGO_PORT");

    String uri = String.format(
        "mongodb://%s:%s@%s:%s/?authSource=admin",
        user, password, host, port);

    try (MongoClient mongoClient = MongoClients.create(uri)) {
      // Wenn Verbindung klappt, werden die Datenbanken aufgelistet
      mongoClient.listDatabaseNames().forEach(dbName -> System.out.println("DB gefunden: " + dbName));
      System.out.println("✅ Verbindung zur MongoDB erfolgreich!");
    } catch (Exception e) {
      System.err.println("❌ Verbindung fehlgeschlagen: " + e.getMessage());
    }
  }

  private static MongoClient client;
  private static MongoDatabase db;
  private static MongoCollection<Document> collection;

  // ---------------------------
  // STATIC INITIALIZATION
  // ---------------------------
  static {
    try {
      client = MongoClients.create("mongodb://localhost:27017/");
      db = client.getDatabase("BigData");
      collection = db.getCollection("books");
    } catch (Exception e) {
      System.err.println("❌ Failed to initialize MongoDB client: " + e.getMessage());
    }
  }

  // ---------------------------
  // CONNECT TO DB (Ping)
  // ---------------------------
  public void connectToDb() {
    try {
      // Check connection by running a simple command
      Document buildInfo = db.runCommand(new Document("buildInfo", 1));
      System.out.println("✅ Successfully connected to MongoDB!");
      System.out.println("MongoDB version: " + buildInfo.getString("version"));
    } catch (Exception e) {
      System.err.println("❌ Could not connect to MongoDB: " + e.getMessage());
    }
  }

  // ---------------------------
  // INSERT MULTIPLE BOOKS
  // ---------------------------
  public void insertIntoDb(List<List<Object>> books) {
    if (books == null || books.isEmpty()) {
      System.out.println("No books to insert.");
      return;
    }

    List<Document> docs = new ArrayList<>();
    for (List<Object> book : books) {
      int id = (int) book.get(0);
      String content = book.get(1).toString();
      docs.add(new Document("id", id).append("content", content));
    }

    try {
      InsertManyResult result = collection.insertMany(docs);
      System.out.println("📚 Inserted " + result.getInsertedIds().size() + " documents into the database.");
    } catch (Exception e) {
      System.err.println("❌ An error occurred while inserting documents: " + e.getMessage());
    }
  }
}
