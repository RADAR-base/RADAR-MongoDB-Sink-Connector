package org.radarcns.pipeline;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Created by nivethika on 23-3-17.
 */
public class EndToEndUtility {

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

}
