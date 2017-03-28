package org.radarcns.integration.commons;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nivethika on 23-3-17.
 */
public class EndToEndUtility {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndUtility.class);

    /**
     * Makes an HTTP request to given URL.
     * @param url end-point
     * @return HTTP Response
     * @throws IOException
     */
    public static Response makeRequest(String url) throws IOException {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();

        Request request = new Request.Builder()
                .header("User-Agent", "Mozilla/5.0")
                .url(url)
                .build();

        return client.newCall(request).execute();
    }


//    public static void checkMongoDB(BasicMockConfig config) {
//        MongoClient mongoClient = null;
//
//        try {
//            List<MongoCredential> credentials = new ArrayList<>();
//            credentials.add(
//                    MongoCredential.createCredential("restapi", "radarcns", "hotstorage".toCharArray()));
//
//
//            MongoClientOptions timeout = MongoClientOptions.builder()
//                    .connectTimeout(11)
////                    .socketTimeout(1)
////                    .serverSelectionTimeout(1)
//                    .build();
//
//            mongoClient = new MongoClient(new ServerAddress("localhost", Integer.parseInt("27017")), credentials, timeout);
////            mongoClient = new MongoClient(Properties.getInstance().getMongoHosts(),credentials);
//
//            if (checkMongoConnection(mongoClient)) {
//
//                LOGGER.info("MongoDB connection established");
//            }
//        } catch (com.mongodb.MongoSocketOpenException exec) {
//            if (mongoClient != null) {
//                mongoClient.close();
//            }
//
//            LOGGER.error(exec.getMessage());
//        }
//    }
//
//    /**
//     * Checks if with the given client and credential is it possible to establish a connection
//     *      towards the MongoDB host.
//     *
//     * @param mongoClient client for MongoDB
//     * @return {@code true} if the connection can be established false otherwise
//     */
//    public static boolean checkMongoConnection(MongoClient mongoClient) {
//        Boolean flag = true;
//        try {
//            for (MongoCredential user : mongoClient.getCredentialsList()) {
//                mongoClient.getDatabase(user.getSource()).runCommand(new Document("ping", 1));
//            }
//
//        } catch (Exception exec) {
//            flag = false;
//
//            if (mongoClient != null) {
//                mongoClient.close();
//            }
//
//            LOGGER.error("Error during connection test",exec);
//        }
//
//        LOGGER.info("MongoDB connection is {}",flag.toString());
//
//        return flag;
//    }
}
