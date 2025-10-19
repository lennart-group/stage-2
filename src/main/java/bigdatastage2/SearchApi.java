package bigdatastage2;

import com.google.gson.Gson;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class SearchApi {
    private static final Gson gson = new Gson();
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<org.bson.Document> booksCollection;
    private static MongoCollection<Document> indexCollection;

    public static void main(String[] args) {

        // Initialize MongoDB connection
        initializeMongoDB();

        // Create Javalin server
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(7003); // Different port from IngestServer

        // Health check status
        app.get("/status", ctx -> {
            Map<String, Object> status = Map.of(
                    "service", "search-service",
                    "status", "running",
                    "database", "connected"
            );
            ctx.result(gson.toJson(status));
        });

        // Main search endpoint: GET /search?q={term}&author={name}&language={code}&year={YYYY}
        app.get("/search", SearchApi::handleSearch);
    }

    private static void initializeMongoDB() {
        try {
            Dotenv dotenv = Dotenv.load();
            String user = dotenv.get("SERVICES_USER");
            String password = dotenv.get("SERVICES_PASSWORD");

            String encUser = URLEncoder.encode(user, StandardCharsets.UTF_8);
            String encPassword = URLEncoder.encode(password, StandardCharsets.UTF_8);

            String connectionString = String.format(
                    "mongodb+srv://%s:%s@bigdataproject.0bhcyld.mongodb.net/?retryWrites=true&w=majority&appName=BigDataProject",
                    encUser, encPassword
            );

            ServerApi serverApi = ServerApi.builder()
                    .version(ServerApiVersion.V1)
                    .build();

            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(connectionString))
                    .serverApi(serverApi)
                    .build();

            mongoClient = MongoClients.create(settings);
            database = mongoClient.getDatabase("BigData");
            booksCollection = database.getCollection("books");
            indexCollection = database.getCollection("invertedIndex");

            // Test connection
            database.runCommand(new Document("ping", 1));
            System.out.println("Successfully connected to MongoDB Atlas!");

        } catch (Exception e) {
            System.err.println("Failed to connect to MongoDB: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void handleSearch(Context ctx) {
        try {
            // Extract query parameters
            String query = ctx.queryParam("q");
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String yearStr = ctx.queryParam("year");

            // Log the request
            System.out.println("New search request: " + ctx.fullUrl());

            // Validate query parameter
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400).result(gson.toJson(Map.of(
                        "error", "Query parameter 'q' is required."
                )));
                System.err.println("Invalid request: Query parameter 'q' is required.");
                return;
            }

            // Search for books containing the search term(s)
            List<Integer> bookIdsFromIndex = searchTerm(query);

            if (bookIdsFromIndex.isEmpty()) {
                ctx.result(gson.toJson(createEmptyResponse(query, author, language, yearStr)));
                System.out.println("Request successfully completed. No results found.");
                return;
            }

            // Apply metadata filters
            List<Integer> filteredBookIds = applyMetadataFilters(bookIdsFromIndex, author, language, yearStr);

            // Fetch book details
            List<Map<String, Object>> results = fetchBookDetails(filteredBookIds);

            // Build response
            Map<String, Object> response = buildResponse(query, author, language, yearStr, results);
            ctx.result(gson.toJson(response));
            System.out.println("Request successfully completed. " + results.size() + " results found.");

        } catch (Exception e) {
            System.err.println("Error in search: " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).result(gson.toJson(Map.of(
                    "error", "Internal server error: " + e.getMessage()
            )));
        }
    }

    /* Searches the inverted index for books containing all terms in the query. */
    private static List<Integer> searchTerm(String query) {
        String[] terms = query.toLowerCase().trim().split("\\s+");

        if (terms.length == 0) {
            return new ArrayList<>();
        }

        // Get postings for first term
        List<Integer> result = getPostingsForTerm(terms[0]);

        // Intersect with postings for remaining terms
        for (int i = 1; i < terms.length; i++) {
            List<Integer> nextPostings = getPostingsForTerm(terms[i]);
            result = intersection(result, nextPostings);

            if (result.isEmpty()) {
                break; // No need to continue if intersection is empty
            }
        }

        return result;
    }

    /* Gets the list of book IDs (postings) for a single term from the inverted index. */
    private static List<Integer> getPostingsForTerm(String term) {
        try {
            Document indexDoc = indexCollection.find(Filters.eq("term", term)).first();

            if (indexDoc == null) {
                return new ArrayList<>();
            }

            List<?> postings = indexDoc.getList("postings", Integer.class);
            return postings != null ? new ArrayList<>((List<Integer>) postings) :  new ArrayList<>();

        } catch (Exception e) {
            System.err.println("Error fetching postings for term '" + term + "':" + e.getMessage());
            return new ArrayList<>();
        }
    }

    /* Applies metadata filters (author, language, year) to the list of book IDs. */
    private static List<Integer> applyMetadataFilters(List<Integer> bookIds, String author, String language, String yearStr) {
        if (bookIds.isEmpty()) {
            return bookIds;
        }

        // Build MongoDB filter
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.in("id", bookIds));

        if (author != null && !author.trim().isEmpty()) {
            filters.add(Filters.regex("metadata.author", author, "i")); // Case-insensitive
        }

        if (language != null && !language.trim().isEmpty()) {
            filters.add(Filters.regex("metadata.language", language.toLowerCase()));
        }

        if (yearStr != null && !yearStr.trim().isEmpty()) {
            try {
                int year = Integer.parseInt(yearStr);
                filters.add(Filters.eq("metadata.year", year));
            } catch (NumberFormatException e) {
                System.err.println("Invalid year format: " + yearStr);
            }
        }

        Bson combinedFilter = Filters.and(filters);

        // Query books collection
        List<Integer> filteredIds = new ArrayList<>();
        try (MongoCursor<Document> cursor = booksCollection.find(combinedFilter).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                filteredIds.add(document.getInteger("id"));
            }
        }

        return filteredIds;
    }

    /* Fetches full book details for the fiven book IDs. */
    private static List<Map<String, Object>> fetchBookDetails(List<Integer> bookIds) {
        if (bookIds.isEmpty()) {
            return new ArrayList<>();
        }

        List<Map<String, Object>> results = new ArrayList<>();

        try (MongoCursor<Document> cursor = booksCollection
                .find(Filters.in("id", bookIds))
                .iterator()) {

            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document metadata = doc.get("metadata", Document.class);

                if (metadata != null) {
                    Map<String, Object> bookInfo = new HashMap<>();
                    bookInfo.put("book_id", doc.getInteger("id"));
                    bookInfo.put("title", metadata.getString("title"));
                    bookInfo.put("author", metadata.getString("author"));
                    bookInfo.put("language", metadata.getString("language"));
                    bookInfo.put("year", metadata.getInteger("year"));
                    results.add(bookInfo);
                }
            }
        }

        return results;
    }

    /* Returns the intersection of two lists. */
    private static List<Integer> intersection(List<Integer> list1, List<Integer> list2) {
        Set<Integer> set1 = new HashSet<>(list1);
        return list2.stream()
                .filter(set1::contains)
                .collect(Collectors.toList());
    }

    /* Builds the JSON response according to the API spec. */
    private static Map<String, Object> buildResponse(String query, String author, String language, String yearStr, List<Map<String, Object>> results) {
        Map<String, Object> response = new HashMap<>();
        response.put("query", query);

        Map<String, Object> filters = new HashMap<>();
        if (author != null && !author.trim().isEmpty()) {
            filters.put("author", author);
        }
        if (language != null && !language.trim().isEmpty()) {
            filters.put("language", language);
        }
        if (yearStr != null && !yearStr.trim().isEmpty()) {
            filters.put("year", yearStr);
        }
        response.put("filters", filters);

        response.put("count", results.size());
        response.put("results", results);

        return response;
    }

    private static Map<String, Object> createEmptyResponse(String query, String author, String language, String yearStr) {
        return buildResponse(query, author, language, yearStr, new ArrayList<>());
    }
}
