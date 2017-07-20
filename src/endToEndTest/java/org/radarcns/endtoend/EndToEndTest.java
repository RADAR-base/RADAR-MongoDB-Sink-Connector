package org.radarcns.endtoend;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.endtoend.util.ExpectedDocumentFactory;
import org.radarcns.mock.config.BasicMockConfig;
import org.radarcns.endtoend.testcase.QuestionnaireEndToEndTest;
import org.radarcns.endtoend.testcase.RecordCountEndToEndTest;
import org.radarcns.endtoend.testcase.ServerStatusEndToEndTest;
import org.radarcns.endtoend.testcase.UptimeEndToEndTest;
import org.radarcns.endtoend.util.SenderTestCase;
import org.radarcns.endtoend.util.TestCase;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.model.ExpectedValue;
import org.radarcns.mock.model.MockAggregator;

/**
 * MongoDB Sink connector e2e test. It streams randomly generated data into the data pipeline and
 *      then queries MongoDb to check that data has been correctly stored. The check is done using
 *      precomputed expected data.
 */
public class EndToEndTest extends TestCase {

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

    @Test
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    /**
     * Test mock sources.
     */
    public void endToEndTest() throws Exception {
        produceInputFile();

        Map<MockDataConfig, ExpectedValue> expectedValue = MockAggregator.getSimulations(
                config.getData(), dataRoot);

        final Map<MockDataConfig, List<Document>> expectedDocument = produceExpectedDocument(
                expectedValue, new ExpectedDocumentFactory());

        streamToKafka();

        Set<SenderTestCase> testCases = new HashSet<>();
        testCases.add(new QuestionnaireEndToEndTest());
        testCases.add(new RecordCountEndToEndTest());
        testCases.add(new ServerStatusEndToEndTest());
        testCases.add(new UptimeEndToEndTest());

        for (SenderTestCase executor : testCases) {
            executor.send();
        }

        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));

        checkMongoDbConnection();

        fetchMongoDb(expectedDocument);

        for (SenderTestCase executor : testCases) {
            LOGGER.info("Fetching {}", executor.getTopicName());
            Document expectedDoc = executor.getExpectedDocument();
            Document actualDoc = getActualDocumet(executor.getTopicName());

            assertEquals("Expected " + expectedDoc.toJson() + " is not equal to actual "
                    + actualDoc.toJson(), expectedDoc, actualDoc);
        }

        closeMongoDbConnection();
    }
}
