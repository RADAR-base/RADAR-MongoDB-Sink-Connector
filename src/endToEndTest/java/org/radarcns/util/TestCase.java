package org.radarcns.util;

import static org.junit.Assert.assertNotNull;
import static org.radarcns.util.TestUtility.waitInfrastructure;

import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import org.junit.BeforeClass;
import org.radarcns.EndToEndTest;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.mock.config.BasicMockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class TestCase {
    public static final Logger LOGGER = LoggerFactory.getLogger(TestCase.class);

    public static final String CONFIG_FILE = "basic_mock_config.yml";

    //TODO add it to BasicMockConfig. It is used also in the REST-API project.
    public static final long DURATION = 60000;

    public static BasicMockConfig config = null;
    public static File dataRoot;

    public static MongoDatabase hotstorage;
    public static final String SUFFIX = "_output";

    // Latency expressed in second
    public static final long LATENCY = 120;

    public static final String MONGO_CONTAINER = "localhost";
    public static final String MONGO_PORT = "27017";
    public static final String MONGO_USER = "restapi";
    public static final String MONGO_PWD = "radarcns";
    public static final String MONGO_DB = "hotstorage";

    public static final String USER_ID_MOCK = "UserID_0";
    public static final String SOURCE_ID_MOCK = "SourceID_0";

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
}
