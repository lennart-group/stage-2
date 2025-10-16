package bigdatastage2;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import kotlin.Pair;

import java.util.List;
import java.util.Map;

public class SearchApi {
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
//        Javalin app = Javalin.create(config -> {
//            config.http.defaultContentType = "application/json";
//        }).start(7000);
//
//        app.get("/status", ctx -> {
//            Map<String, Object> status = Map.of(
//                    "service", "example-service",
//                    "status", "running"
//            );
//            ctx.result(gson.toJson(status));
//        });
//
//        app.get("/data", SearchApi::handledata);
    }

    /* These terms (words) will be searched in the reversed index.
    *  All the words in the term must be in all books.
    *  This function will return list of book ids, that include all the terms. */
    private static List<Integer> searchTerm(Context ctx, String term) {
        // TODO
    }

    /* This method will get a list of book ids and metadata and it will
    *  filter out those book ids that don't match the metadata.
    *  This will need to validate the metadata from the db, not the index. */
    private static List<Integer> searchMetadata(Context ctx, Pair<String, String> metadata) {
        // TODO
    }

    /* This method will get two lists of book ids and will return only those ids that are in both lists. */
    private static List<Integer> intersection(List<Integer> list1, List<Integer> list2) {
        // TODO
    }

   // private static void handledata(Context ctx) {
//        String filter = ctx.queryParam("filter");
//
//
//        // Example mock dataset
//        List<Map<String, Object>> items = List.of(
//                Map.of("id", 1, "name", "Item A"),
//                Map.of("id", 2, "name", "Item B"),
//                Map.of("id", 3, "name", "Item C")
//        );
//        Map<String, Object> response = Map.of(
//                "filter", filter,
//                "count", items.size(),
//                "items", items
//        );
//       ctx.result(gson.toJson(response));
//    }
}
