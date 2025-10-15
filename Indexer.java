import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class Indexer {

    // ----------------------
    // PATHS & CONTROL FILES
    // ----------------------
    private static final Path CONTROL_DIR = Paths.get("control");
    private static final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");

    // ----------------------
    // DATABASES
    // ----------------------
    private static Connection sqlConn;
    private static Statement sqlStmt;
    private static MongoCollection<Document> mongoIndex;
    private static MongoCollection<Document> booksCollection;

    public static void main(String[] args) {
        try {
            setupControlDir();
            setupSQLite();
            setupMongo();

            reindexAllBooks();

            System.out.println("🎉 Indexing complete.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ----------------------
    // SETUP METHODS
    // ----------------------
    private static void setupControlDir() throws IOException {
        if (!Files.exists(CONTROL_DIR)) {
            Files.createDirectories(CONTROL_DIR);
        }
    }

    private static void setupSQLite() throws SQLException {
        sqlConn = DriverManager.getConnection("jdbc:sqlite:datamarts/inverted_index.db");
        sqlStmt = sqlConn.createStatement();
        sqlStmt.executeUpdate("""
            CREATE TABLE IF NOT EXISTS inverted_index (
                term TEXT PRIMARY KEY,
                postings TEXT
            )
        """);
        sqlConn.commit();
    }

    private static void setupMongo() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/");
        MongoDatabase db = mongoClient.getDatabase("repository_connection"); // same as your Python import
        mongoIndex = db.getCollection("invertedIndex");
        booksCollection = db.getCollection("books");
        mongoIndex.createIndex(new Document("term", 1));
    }

    // ----------------------
    // TOKENIZER
    // ----------------------
    private static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        Matcher matcher = Pattern.compile("\\b[a-z]{2,}\\b").matcher(text.toLowerCase());
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    // ----------------------
    // UPDATE FUNCTIONS
    // ----------------------
    private static void updateSQL(String term, int bookId) throws SQLException {
        PreparedStatement psSelect = sqlConn.prepareStatement(
            "SELECT postings FROM inverted_index WHERE term=?");
        psSelect.setString(1, term);
        ResultSet rs = psSelect.executeQuery();

        List<Integer> postings = new ArrayList<>();
        if (rs.next()) {
            String json = rs.getString("postings");
            JSONArray arr = new JSONArray(json);
            for (int i = 0; i < arr.length(); i++) {
                postings.add(arr.getInt(i));
            }
        }

        if (!postings.contains(bookId)) {
            postings.add(bookId);
            String newJson = new JSONArray(postings).toString();

            if (rs.isBeforeFirst() || rs.isAfterLast()) {
                // insert new
                PreparedStatement psInsert = sqlConn.prepareStatement(
                    "INSERT INTO inverted_index (term, postings) VALUES (?, ?)");
                psInsert.setString(1, term);
                psInsert.setString(2, newJson);
                psInsert.executeUpdate();
            } else {
                // update existing
                PreparedStatement psUpdate = sqlConn.prepareStatement(
                    "REPLACE INTO inverted_index (term, postings) VALUES (?, ?)");
                psUpdate.setString(1, term);
                psUpdate.setString(2, newJson);
                psUpdate.executeUpdate();
            }
        }
    }

    private static void updateMongo(String term, int bookId) {
        mongoIndex.updateOne(
            Filters.eq("term", term),
            Updates.addToSet("postings", bookId),
            new com.mongodb.client.model.UpdateOptions().upsert(true)
        );
    }

    // ----------------------
    // INDEXING LOGIC
    // ----------------------
    private static void processBook(int bookId, String text) throws Exception {
        Set<String> words = tokenize(text);

        sqlConn.setAutoCommit(false);
        for (String term : words) {
            updateSQL(term, bookId);
            updateMongo(term, bookId);
        }
        sqlConn.commit();

        markIndexed(bookId);
        System.out.printf("✅ Indexed book %d (%d unique terms).%n", bookId, words.size());
    }

    private static void markIndexed(int bookId) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(INDEXED_FILE, 
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(String.valueOf(bookId));
            writer.newLine();
        }
    }

    private static boolean alreadyIndexed(int bookId) throws IOException {
        if (!Files.exists(INDEXED_FILE)) return false;
        List<String> lines = Files.readAllLines(INDEXED_FILE);
        return lines.contains(String.valueOf(bookId));
    }

    // ----------------------
    // MAIN LOOP
    // ----------------------
    private static void reindexAllBooks() throws Exception {
        for (Document doc : booksCollection.find()) {
            int bookId = doc.getInteger("id");
            if (alreadyIndexed(bookId)) continue;
            String text = doc.getString("content");
            processBook(bookId, text);
        }
    }
}
