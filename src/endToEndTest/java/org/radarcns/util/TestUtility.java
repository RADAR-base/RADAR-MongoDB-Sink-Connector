package org.radarcns.util;

import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.radarcns.sink.util.MongoConstants.ID;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import okhttp3.Response;
import org.bson.Document;
import org.radarcns.mock.MockProducer;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.data.CsvGenerator;
import org.radarcns.mock.data.MockRecordValidator;
import org.radarcns.mock.model.ExpectedValue;
import org.radarcns.producer.rest.RestClient;

/*
 * Copyright 2017 King's College London and The Hyve
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
public class TestUtility extends TestCase{

    /**
     * Checks if the test bed is ready to accept data.
     */
    public static void waitInfrastructure() throws InterruptedException, MalformedURLException {
        LOGGER.info("Waiting on infrastructure ... ");

        List<String> expectedTopics = new LinkedList<>();
        for (MockDataConfig sender : config.getData()) {
            expectedTopics.add(sender.getTopic());
            expectedTopics.add(sender.getTopic().concat(SUFFIX));
        }

        int retry = 60;
        long sleep = 1000;

        try (RestClient client = new RestClient(config.getRestProxy())) {
            for (int i = 0; i < retry; i++) {
                try (Response response = client.request("topics")) {
                    if (response.code() == 200) {
                        String topics = response.body().string();
                        String[] topicArray = topics.substring(1, topics.length() - 1).replace(
                            "\"", "").split(",");

                        expectedTopics.removeAll(asList(topicArray));

                        if (expectedTopics.isEmpty()) {
                            break;
                        }
                    }
                } catch (IOException ex) {
                    LOGGER.info("Error while waiting infrastructure", ex);
                }

                Thread.sleep(sleep * (i + 1));
            }
        }

        assertEquals("missing " + expectedTopics, 0, expectedTopics.size());
    }

    /**
     * Generates new random CSV files.
     */
    public static void produceInputFile()
            throws IOException, ClassNotFoundException, NoSuchMethodException,
        InvocationTargetException, ParseException, IllegalAccessException {
        LOGGER.info("Generating CSV files ...");
        for (MockDataConfig config : config.getData()) {
            new CsvGenerator().generate(config, DURATION, dataRoot);
            new MockRecordValidator(config, DURATION, dataRoot).validate();
        }
    }

    /**
     * Starting from the expected values computed using the available CSV files, it computes all
     * the expected Datasets used to test REST-API.
     *
     * @see ExpectedValue
     * @see ExpectedDocumentFactory
     */
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public static Map<MockDataConfig, List<Document>> produceExpectedDocument(Map<MockDataConfig,
            ExpectedValue> expectedValue, ExpectedDocumentFactory expectedDocumentFactory)
            throws Exception {
        LOGGER.info("Computing expected dataset ...");
        int testCase = config.getData().size();

        assertEquals(testCase, expectedValue.size());
        Map<MockDataConfig, List<Document>> docMap = new HashMap<>();
        for (MockDataConfig key : expectedValue.keySet()) {
            List<Document> documents = expectedDocumentFactory.produceExpectedData(
                expectedValue.get(key));
            docMap.put(key, documents);
        }
        return docMap;
    }

    /**
     * Streams data stored in CSV files into Kafka.
     */
    public static void streamToKafka() throws IOException, InterruptedException {
        LOGGER.info("Streaming data into Kafka ...");
        MockProducer producer = new MockProducer(config, dataRoot);
        producer.start();
        producer.shutdown();
    }

    /**
     * Checks if a MongoDb Client can be instantiated.
     */
    public static void checkMongoDbConnection() {
        List<ServerAddress> servers = asList(new ServerAddress(
            MONGO_CONTAINER.concat(":").concat(MONGO_PORT)));
        List<MongoCredential> credential = singletonList(
            MongoCredential.createCredential(MONGO_USER, MONGO_DB, MONGO_PWD.toCharArray()));

        MongoClient client = new MongoClient(servers, credential,
            MongoClientOptions.builder().serverSelectionTimeout(1000).build());

        hotstorage = client.getDatabase(MONGO_DB);
        try {
            hotstorage.runCommand(new Document("ping", 1));

        } catch (Exception ex) {
            LOGGER.error("Error during MongoDB connection test", ex);
            if (client != null) {
                client.close();
            }
        }

    }

    /**
     * Queries MongoDb and checks if expected data has been correctly generated.
     */
    public static void fetchMongoDb(Map<MockDataConfig, List<Document>> expectedDocument) {
        for (MockDataConfig sensor : expectedDocument.keySet()) {
            FindIterable<Document> collection = hotstorage.getCollection(
                sensor.getTopic().concat(SUFFIX)).find().sort(ascending(ID));
            List<Document> expectedDocs = expectedDocument.get(sensor);
            MongoCursor cursor = collection.iterator();

            int count = 0;

            while (cursor.hasNext()) {
                Document actualDoc = (Document) cursor.next();
                Document expectedDoc = expectedDocs.get(count);
                assertDocument(expectedDoc, actualDoc, sensor.getMaximumDifference());
                count++;
            }

            assertEquals(expectedDocs.size(), count);
        }
    }

    /**
     * Checks if the two given documents are equals. Numeric values are compared using a constant
     * representing the maximum delta for which both numbers are still considered equal.
     */
    public static void assertDocument(Document expected, Document actual, Double delta) {
        LOGGER.info("Expected: {}", expected.toJson());
        LOGGER.info("Actual: {}", actual.toJson());

        assertEquals(expected.keySet(), actual.keySet());

        for (String key : expected.keySet()) {
            checkClasses(expected, actual);

            if (expected.get(key) instanceof Double) {
                assertEquals(expected.getDouble(key), actual.getDouble(key), delta);
            } else if (expected.get(key) instanceof Long) {
                assertEquals(expected.getLong(key), actual.getLong(key), delta);
            } else if (expected.get(key) instanceof Integer) {
                assertEquals(expected.getInteger(key), actual.getInteger(key), delta);
            } else if (expected.get(key) instanceof List) {
                List<Document> expectedList = (List<Document>) expected.get(key);
                List<Document> actualList = (List<Document>) actual.get(key);

                assertEquals(expectedList.size(), actualList.size());

                for (int i = 0; i < expectedList.size(); i++) {
                    assertDocument(expectedList.get(i), actualList.get(i), delta);
                }
            } else {
                assertEquals(expected.get(key), actual.get(key));
            }
        }
    }

    /**
     * Checks if the classes of inputs can be considered equals.
     */
    public static void checkClasses(Document expected, Document actual) {
        if (expected.getClass().getCanonicalName().equals("java.util.Arrays.ArrayList")
            && actual.getClass().getCanonicalName().equals("java.util.ArrayList")) {
            return;
        } else {
            assertEquals(expected.getClass(), actual.getClass());
        }
    }

}
