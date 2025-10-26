package bigdatastage2;

import io.github.cdimascio.dotenv.Dotenv;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.*;
import com.mongodb.client.result.InsertManyResult;
import org.bson.Document;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.net.URLEncoder;

public class RepositoryConnection {

  public static MongoDatabase[] connectToDB() throws FileNotFoundException {

    Dotenv dotenv = Dotenv.load();

    String user = dotenv.get("SERVICES_USER");
    String password = dotenv.get("SERVICES_PASSWORD");

    String enc_user = "";
    try {
      enc_user = URLEncoder.encode(user, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    String enc_password = "";
    try {
      enc_password = URLEncoder.encode(password, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    String connectionString = String.format("mongodb+srv://%s:%s@bigdataproject.0bhcyld.mongodb.net/?retryWrites=true&w=majority&appName=BigDataProject", enc_user, enc_password);

    ServerApi serverApi = ServerApi.builder()
        .version(ServerApiVersion.V1)
        .build();

    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .serverApi(serverApi)
        .build();

    // Create a new client and connect to the server
    MongoClient mongoClient = MongoClients.create(settings);
      try {
        // Send a ping to confirm a successful connection
        MongoDatabase booksDatabase = mongoClient.getDatabase("BigData");
        booksDatabase.runCommand(new Document("ping", 1));
        MongoDatabase indexDatabase = mongoClient.getDatabase("invertedIndex");
        indexDatabase.runCommand(new Document("ping", 1));
        System.out.println("Pinged your deployment. You successfully connected to MongoDB!");
        return new MongoDatabase[]{booksDatabase, indexDatabase};
      } catch (MongoException e) {
        System.err.println("Failed to connect to MongoDB!" + e.getMessage());
        e.printStackTrace();
        System.exit(1);
      }
    return null;
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
