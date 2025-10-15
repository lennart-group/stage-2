import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

import repository_connection.RepositoryConnection;

public class GutenbergDownloader {

    // Used to download only new items from the source (mock value)
    private static int lastLocalId = 0;

    // ---------------------------
    // GET BOOK BY ID
    // ---------------------------
    public static List<Object> getBook(int id) {
        try {
            // Download raw text from Gutenberg
            String url = "https://www.gutenberg.org/files/" + id + "/" + id + "-0.txt";
            String rawBook = fetchText(url);

            if (rawBook == null || rawBook.isEmpty()) {
                System.out.println("⚠️ Book ID " + id + " not found.");
                return null;
            }

            // Remove Gutenberg header/footer
            String cleanBook = stripHeaders(rawBook);
            return Arrays.asList(id, cleanBook);

        } catch (Exception e) {
            System.err.println("❌ Error fetching book ID " + id + ": " + e.getMessage());
            return null;
        }
    }

    // ---------------------------
    // STRIP HEADERS / FOOTERS
    // ---------------------------
    private static String stripHeaders(String text) {
        // Rough equivalent of gutenbergpy.textget.strip_headers()
        String patternStart = "\\*\\*\\*\\s*START OF.*?\\*\\*\\*";
        String patternEnd = "\\*\\*\\*\\s*END OF.*?\\*\\*\\*";

        String result = text.replaceAll("(?s)^.*?" + patternStart, "");
        result = result.replaceAll("(?s)" + patternEnd + ".*$", "");
        return result.trim();
    }

    // ---------------------------
    // FETCH TEXT FROM URL
    // ---------------------------
    private static String fetchText(String urlString) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new URL(urlString).openStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null)
                sb.append(line).append("\n");
            return sb.toString();
        } catch (IOException e) {
            return "";
        }
    }

    // ---------------------------
    // GET LATEST BOOK ID
    // ---------------------------
    public static int getLastReleasedBookId() {
        try {
            String searchUrl = "https://gutenberg.org/ebooks/search/?sort_order=release_date";
            Document doc = Jsoup.connect(searchUrl).get();
            Element results = doc.selectFirst("ul.results");
            if (results != null) {
                Element link = results.selectFirst("li.booklink a.link");
                if (link != null) {
                    String href = link.attr("href"); // e.g. "/ebooks/74321"
                    Matcher m = Pattern.compile("/ebooks/(\\d+)").matcher(href);
                    if (m.find()) {
                        return Integer.parseInt(m.group(1));
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Failed to fetch last released ID: " + e.getMessage());
        }
        return -1;
    }

    // ---------------------------
    // GET BOOKS IN RANGE
    // ---------------------------
    public static List<List<Object>> getBooks(int idFirst, int idLast) {
        List<List<Object>> books = new ArrayList<>();
        for (int id = idFirst; id < idLast; id++) {
            List<Object> book = getBook(id);
            if (book != null)
                books.add(book);
        }
        return books;
    }

    // ---------------------------
    // GET NEW BOOKS
    // ---------------------------
    public static List<List<Object>> getNewBooks() {
        int start = lastLocalId;
        int end = getLastReleasedBookId();
        return getBooks(start, end);
    }

    // ---------------------------
    // STORE BOOKS INTO DB
    // ---------------------------
    public static void storeBooks(int idFirst, int idLast) {
        List<List<Object>> books = getBooks(idFirst, idLast);

        RepositoryConnection repo = new RepositoryConnection();
        repo.connectToDb();
        repo.insertIntoDb(books);

        System.out.println("💾 Stored " + books.size() + " books into database.");
    }

    // ---------------------------
    // MAIN (for testing)
    // ---------------------------
    public static void main(String[] args) {
        storeBooks(4, 6);
    }
}
