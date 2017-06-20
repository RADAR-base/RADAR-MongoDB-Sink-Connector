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

package org.radarcns;

import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.radarcns.sink.mongodb.util.MongoConstants.ID;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.Response;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.integration.ExpectedDocumentFactory;
import org.radarcns.mock.MockProducer;
import org.radarcns.mock.config.BasicMockConfig;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.data.CsvGenerator;
import org.radarcns.mock.data.MockRecordValidator;
import org.radarcns.mock.model.ExpectedValue;
import org.radarcns.mock.model.MockAggregator;
import org.radarcns.producer.rest.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB Sink connector e2e test
 */
public class EndToEndTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndTest.class);

    //private static final String USER_ID_MOCK = "UserID_0";
    //private static final String SOURCE_ID_MOCK = "SourceID_0";

    private final ExpectedDocumentFactory expectedDocumentFactory = new ExpectedDocumentFactory();

    private static final String CONFIG_FILE = "basic_mock_config.yml";

    //TODO add it to BasicMockConfig. It is used also in the REST-API project.
    private static final long DURATION = 60000;

    private static BasicMockConfig config = null;
    private static File dataRoot;

    private static MongoDatabase hotstorage;
    private static final String SUFFIX = "_output";

    // Latency expressed in second
    private static final long LATENCY = 120;

    private static final String MONGO_CONTAINER = "localhost";
    private static final String MONGO_PORT = "27017";
    private static final String MONGO_USER = "restapi";
    private static final String MONGO_PWD = "radarcns";
    private static final String MONGO_DB = "hotstorage";

    /**
     * Test initialisation. It loads the config file and waits that the infrastructure is ready
     *      to accept requests.
     */
    @BeforeClass
    public static void setUpClass() throws IOException, InterruptedException {
        URL configResource = EndToEndTest.class.getClassLoader().getResource(CONFIG_FILE);
        assertNotNull(configResource);
        File configFile = new File(configResource.getFile());
        config = new YamlConfigLoader().load(configFile, BasicMockConfig.class);
        dataRoot = configFile.getAbsoluteFile().getParentFile();

        waitInfrastructure();
    }

    /**
     * Checks if the test bed is ready to accept data.
     */
    private static void waitInfrastructure() throws InterruptedException, MalformedURLException {
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

    @Test
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void endToEndTest() throws Exception {
        produceInputFile();

        Map<MockDataConfig, ExpectedValue> expectedValue = MockAggregator.getSimulations(
                config.getData(), dataRoot);

        final Map<MockDataConfig, List<Document>> expectedDocument = produceExpectedDocument(
                expectedValue);

        streamToKafka();

        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));

        checkMongoDbConnection();

        fetchMongoDb(expectedDocument);
    }

    /**
     * Generates new random CSV files.
     */
    private void produceInputFile()
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
     * @see {@link ExpectedValue}
     */
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    private Map<MockDataConfig, List<Document>> produceExpectedDocument(Map<MockDataConfig,
            ExpectedValue> expectedValue) throws Exception {
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
    private void streamToKafka() throws IOException, InterruptedException {
        LOGGER.info("Streaming data into Kafka ...");
        MockProducer producer = new MockProducer(config, dataRoot);
        producer.start();
        producer.shutdown();
    }

    /**
     * Checks if a MongoDb Client can be instantiated.
     */
    private void checkMongoDbConnection() {
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

    private void fetchMongoDb(Map<MockDataConfig, List<Document>> expectedDocument) {
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

    private void assertDocument(Document expected, Document actual, Double delta) {

        LOGGER.info("Expected: {}", expected.toJson());
        LOGGER.info("Actual: {}", actual.toJson());

        assertEquals(expected.keySet(), actual.keySet());

        for (String key : expected.keySet()) {
            assertEquals(expected.get(key), actual.get(key));
        }

    }

//    private void compareSingleItemDocument(Document expected, Document actual, double delta) {
//        for (String key : expected.keySet()) {
//            //assert both documents have same headers
//            assertNotNull(actual.get(key));
//            switch (key) {
//                case ID:
//                case "user":
//                case "source":
//                    assertEquals(expected.get(key), actual.get(key));
//                    break;
//                case "start":
//                case "end":
//                    assertEquals((Date) expected.get(key), (Date) actual.get(key));
//                    break;
//                case "QUARTILES":
//                    assertQuartiles(expected, actual);
//                    break;
//                default:
//                    assertEquals((Double) expected.get(key), (Double) actual.get(key), delta);
//                    break;
//            }
//        }
//
//    }
//
//    private void assertQuartiles(Document expected, Document actual, double delta) {
//        List expectedQuartile = (List) expected.get(Stat.QUARTILES.getParam());
//        ArrayList<Document> actualQuartile = (ArrayList<Document>)
//                actual.get(Stat.QUARTILES.getParam());
//        for (int i = 0; i < expectedQuartile.size(); i++) {
//            Document act = actualQuartile.get(i);
//            Document exp = (Document) expectedQuartile.get(i);
//
//            for (String key : exp.keySet()) {
//                assertNotNull(act.get(key));
//                assertEquals((Double) exp.get(key), (Double) act.get(key), delta);
//            }
//        }
//    }
//
//    private void compareArrayItemDocument(Document expected, Document actual, double delta) {
//        for (String key : expected.keySet()) {
//            assertNotNull(actual.get(key));
//            switch (key) {
//                case ID:
//                case USER:
//                case SOURCE:
//                    assertEquals(expected.get(key), actual.get(key));
//                    break;
//                case START:
//                case END:
//                    assertEquals((Date) expected.get(key), (Date) actual.get(key));
//                    break;
//                case "QUARTILES":
//                    assertAccelerationQuartiles(expected, actual);
//                    break;
//                default:
//                    assertAccelerometerDocuments((ArrayList) expected.get(key),
//                            (Document) actual.get(key));
//                    break;
//            }
//        }
//    }
//
//    private void assertAccelerationQuartiles(Document expected, Document actual) {
//        Document actualQuartileDoc = (Document) actual.get(Stat.QUARTILES.getParam());
//        Document expectedQuartile = (Document) expected.get(Stat.QUARTILES.getParam());
//        for (String axis : expectedQuartile.keySet()) {
//            assertNotNull(actualQuartileDoc.get(axis));
//            assertAccelerationAxisQuartileValues((List) expectedQuartile.get(axis),
//                    (List) actualQuartileDoc.get(axis));
//        }
//    }
//
//    private void assertAccelerationAxisQuartileValues(List expected, List actual, double delta) {
//        assertEquals(expected.size(), actual.size());
//        assertEquals((Double) ((Document) expected.get(0)).get(FIRST_QUARTILE),
//                (Double) ((Document) actual.get(0)).get(FIRST_QUARTILE), delta);
//        assertEquals((Double) ((Document) expected.get(1)).get(SECOND_QUARTILE),
//                (Double) ((Document) actual.get(1)).get(SECOND_QUARTILE), delta);
//        assertEquals((Double) ((Document) expected.get(2)).get(THIRD_QUARTILE),
//                (Double) ((Document) actual.get(2)).get(THIRD_QUARTILE), delta);
//    }
//
//    private void assertAccelerometerDocuments(ArrayList accelerationList,
//          Document actualDocument) {
//        assertEquals((Double) accelerationList.get(0),
//                  (Double) actualDocument.get(X_LABEL), delta);
//        assertEquals((Double) accelerationList.get(1),
//                   (Double) actualDocument.get(Y_LABEL), delta);
//        assertEquals((Double) accelerationList.get(2),
//                    (Double) actualDocument.get(Z_LABEL), delta);
//    }
}
