/*
 * Copyright 2017 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.pipeline;

import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.integration.ExpectedDocumentFactory;
import org.radarcns.integration.aggregator.MockAggregator;
import org.radarcns.integration.model.ExpectedValue;
import org.radarcns.mock.BasicMockConfig;
import org.radarcns.mock.CsvGenerator;
import org.radarcns.mock.MockDataConfig;
import org.radarcns.mock.MockProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MongoDBSinkEndToEndTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBSinkEndToEndTest.class);

    private static final String BASIC_MOCK_CONFIG_FILE = "basic_mock_config.yml";

    private static BasicMockConfig config = null;

    private static MongoDatabase hotstorage;

    private static ExpectedDocumentFactory expectedDocumentFactory;

    public static final String ID = "_id";

    // Latency expressed in second
    private static final long LATENCY = 60;

    private static final double DELTA = Math.pow(10.0, -1.0 * 10);

    @Test
    public void testMongoDBSinkPipeline()
            throws Exception {
        expectedDocumentFactory = new ExpectedDocumentFactory();

        getBasicMockConfig();
        assertNotNull(config);

        waitForInfrastructureBootUp();

        generateCsvInputFiles();

        Map<MockDataConfig, ExpectedValue> expectedValue = MockAggregator.
                getSimulations(getBasicMockConfig().getData());

        Map<MockDataConfig, List<Document>> expectedDocument = produceExpectedDocument(
                expectedValue);

        streamCsvDataToKafka();

        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));

        checkMongoDbConnection();

        assetDataWithExpectedDocuments(expectedDocument);


    }

    private void assetDataWithExpectedDocuments(
            Map<MockDataConfig, List<Document>> expectedDocument) {
        String collection_suffix = "_output";
        for (MockDataConfig key : expectedDocument.keySet()) {
            FindIterable<Document> collection = hotstorage.
                    getCollection(key.getTopic() + collection_suffix).find().sort(ascending(ID));
            List<Document> expecetedDocs = expectedDocument.get(key);
            MongoCursor cursor = collection.iterator();
            int count = 0;
            while (cursor.hasNext()) {
                Document actualDoc = (Document) cursor.next();
                Document expectedDoc = expecetedDocs.get(count);
                assertDocument(key.getSensor(), expectedDoc, actualDoc);
                count++;
            }
            LOGGER.info("Expected count {} and Actual count {}  ", expecetedDocs.size(), count);

            assertEquals(expecetedDocs.size(), count);
        }


    }

    private void assertDocument(String sensorType, Document expected, Document actual) {
        switch (sensorType) {
            case "ACCELEROMETER":
                compareArrayItemDocument(expected, actual);
                break;
            default:
                compareSingleItemDocument(expected, actual);
                break;
        }
    }

    private void compareSingleItemDocument(Document expected, Document actual) {
        for (String key : expected.keySet()) {
            //assert both documents have same headers
            assertNotNull(actual.get(key));
            switch (key) {
                case "_id":
                case "user":
                case "source":
                    assertEquals(expected.get(key), actual.get(key));
                    break;
                case "start":
                case "end":
                    assertEquals((Date) expected.get(key), (Date) actual.get(key));
                    break;
                case "quartile":
                    assertQuartiles(expected, actual);
                    break;
                default:
                    assertEquals((Double) expected.get(key), (Double) actual.get(key), DELTA);
                    break;
            }
        }

    }

    private void assertQuartiles(Document expected, Document actual) {
        List expectedQuartile = (List) expected.get("quartile");
        ArrayList<Document> actualQuartile = (ArrayList<Document>) actual.get("quartile");
        for (int i = 0; i < expectedQuartile.size(); i++) {
            Document act = actualQuartile.get(i);
            Document exp = (Document) expectedQuartile.get(i);

            for (String key : exp.keySet()) {
                assertNotNull(act.get(key));
                assertEquals((Double) exp.get(key), (Double) act.get(key), DELTA);
            }
        }
    }

    private void compareArrayItemDocument(Document expected, Document actual) {
        for (String key : expected.keySet()) {
            assertNotNull(actual.get(key));
            switch (key) {
                case "_id":
                case "user":
                case "source":
                    assertEquals(expected.get(key), actual.get(key));
                    break;
                case "start":
                case "end":
                    assertEquals((Date) expected.get(key), (Date) actual.get(key));
                    break;
                case "quartile":
                    assertAccelerationQuartiles(expected, actual);
                    break;
                default:
                    assertAccelerometerDocuments((ArrayList) expected.get(key),
                            (Document) actual.get(key));
            }
        }
    }

    private void assertAccelerationQuartiles(Document expected, Document actual) {
        Document actualQuartileDoc = (Document) actual.get("quartile");
        Document expectedQuartile = (Document) expected.get("quartile");
        for (String axis : expectedQuartile.keySet()) {
            assertNotNull(actualQuartileDoc.get(axis));
            assertAccelerationAxisQuartileValues((List) expectedQuartile.get(axis),
                    (List) actualQuartileDoc.get(axis));
        }
    }

    private void assertAccelerationAxisQuartileValues(List expected, List actual) {
        assertEquals(expected.size(), actual.size());
        assertEquals((Double) ((Document) expected.get(0)).get("25"),
                (Double) ((Document) actual.get(0)).get("25"), DELTA);
        assertEquals((Double) ((Document) expected.get(1)).get("50"),
                (Double) ((Document) actual.get(1)).get("50"), DELTA);
        assertEquals((Double) ((Document) expected.get(2)).get("75"),
                (Double) ((Document) actual.get(2)).get("75"), DELTA);
    }

    private void assertAccelerometerDocuments(ArrayList accelerationList, Document actualDocument) {
        assertEquals((Double) accelerationList.get(0), (Double) actualDocument.get("x"), DELTA);
        assertEquals((Double) accelerationList.get(1), (Double) actualDocument.get("y"), DELTA);
        assertEquals((Double) accelerationList.get(2), (Double) actualDocument.get("z"), DELTA);
    }

    private void checkMongoDbConnection() {
        MongoClient client = new MongoClient(
                asList(new ServerAddress("localhost:27017")),
                singletonList(MongoCredential.createCredential("restapi",
                        "hotstorage",
                        "radarcns".toCharArray())),
                MongoClientOptions.builder().serverSelectionTimeout(1000).build());
        hotstorage = client.getDatabase("hotstorage");
        try {
            hotstorage.runCommand(new Document("ping", 1));

        } catch (Exception ex) {
            LOGGER.error("Error during MongoDB connection test", ex);

            if (client != null) {
                client.close();
            }
        }

    }

    private void streamCsvDataToKafka() throws IOException, InterruptedException {
        MockProducer producer = new MockProducer(getBasicMockConfig());
        LOGGER.info("Streaming data into Kafka ...");
        producer.start();
        producer.shutdown();
    }

    /**
     * Starting from the expected values computed using the available CSV files, it computes all
     * the expected Datasets used to test REST-API.
     *
     * @see {@link ExpectedValue}
     */
    private Map<MockDataConfig, List<Document>> produceExpectedDocument(
            Map<MockDataConfig, ExpectedValue> expectedValue)
            throws Exception {
        LOGGER.info("Computing expected dataset ...");
        int testCase = getBasicMockConfig().getData().size();

        assertEquals(testCase, expectedValue.size());
        Map<MockDataConfig, List<Document>> docMap = new HashMap<>();
        for (MockDataConfig key : expectedValue.keySet()) {
            List<Document> documents = (List<Document>) expectedDocumentFactory.
                    produceExpectedData(expectedValue.get(key));
            docMap.put(key, documents);
        }
        return docMap;
    }


    private void generateCsvInputFiles() throws IOException {
        LOGGER.info("Generating CVS files ...");
        File parentFile = new File(
                MongoDBSinkEndToEndTest.class.getClassLoader().
                        getResource(BASIC_MOCK_CONFIG_FILE).getFile()
        );
        for (MockDataConfig config : getBasicMockConfig().getData()) {
            CsvGenerator.generate(config, (long) 30, parentFile);
        }
    }

    /**
     * Checks if the test bed is ready to accept data.
     */
    private void waitForInfrastructureBootUp() throws InterruptedException {
        LOGGER.info("Waiting infrastructure ... ");
        int retry = 60;
        long sleep = 1000;
        int count = 0;

        List<String> expectedTopics = new LinkedList<>();
        expectedTopics.add("android_empatica_e4_acceleration");
        expectedTopics.add("android_empatica_e4_acceleration_output");
        expectedTopics.add("android_empatica_e4_battery_level");
        expectedTopics.add("android_empatica_e4_battery_level_output");
        expectedTopics.add("android_empatica_e4_blood_volume_pulse");
        expectedTopics.add("android_empatica_e4_blood_volume_pulse_output");
        expectedTopics.add("android_empatica_e4_electrodermal_activity");
        expectedTopics.add("android_empatica_e4_electrodermal_activity_output");
        expectedTopics.add("android_empatica_e4_heartrate");
        expectedTopics.add("android_empatica_e4_inter_beat_interval");
        expectedTopics.add("android_empatica_e4_inter_beat_interval_output");
        expectedTopics.add("android_empatica_e4_sensor_status");
        expectedTopics.add("android_empatica_e4_sensor_status_output");
        expectedTopics.add("android_empatica_e4_temperature");
        expectedTopics.add("android_empatica_e4_temperature_output");
        expectedTopics.add("application_server_status");
        expectedTopics.add("application_record_counts");
        expectedTopics.add("application_uptime");

        for (int i = 0; i < retry; i++) {
            count = 0;

            Response response = null;
            try {
                response = makeRequest(
                        config.getRestProxy().getUrlString() + "/topics");
                if (response.code() == 200) {
                    String topics = response.body().string().toString();
                    String[] topicArray = topics.substring(1, topics.length() - 1).
                            replace("\"", "").split(",");

                    for (String topic : topicArray) {
                        if (expectedTopics.contains(topic)) {
                            count++;
                        }
                    }

                    if (count == expectedTopics.size()) {
                        break;
                    }
                }
            } catch (IOException exec) {
                LOGGER.info("Error while waiting infrastructure", exec);
            }

            Thread.sleep(sleep * (i + 1));
        }

        assertEquals(expectedTopics.size(), count);
    }


    private static BasicMockConfig getBasicMockConfig() {
        if (config == null) {

            try {
                config = new YamlConfigLoader().load(
                        new File(
                                MongoDBSinkEndToEndTest.class.getClassLoader().
                                        getResource(BASIC_MOCK_CONFIG_FILE).getFile()
                        ), BasicMockConfig.class);
            } catch (IOException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }
        return config;
    }

    /**
     * Makes an HTTP request to given URL.
     *
     * @param url end-point
     * @return HTTP Response
     */
    public static Response makeRequest(String url) throws IOException {
        OkHttpClient client = new OkHttpClient.Builder().
                connectTimeout(30, TimeUnit.SECONDS).
                writeTimeout(30, TimeUnit.SECONDS).
                readTimeout(30, TimeUnit.SECONDS).
                build();

        Request request = new Request.Builder().
                header("User-Agent", "Mozilla/5.0").
                url(url).
                build();

        return client.newCall(request).execute();
    }
}
