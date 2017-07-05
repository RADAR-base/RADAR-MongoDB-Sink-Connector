package org.radarcns.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.Test;

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
public abstract class SenderTestCase extends TestCase {

    @Test
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    /**
     * Test a specific {@link org.apache.avro.specific.SpecificRecord}.
     */
    public void test() throws Exception {
        send();

        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));

        Document expectedDoc = getExpectedDocument();
        LOGGER.info("Expected: {}", expectedDoc.toJson());

        Document actualDoc = TestUtility.getActualDocumet(getTopicName());
        LOGGER.info("Actual: {}", actualDoc.toJson());

        assertEquals(expectedDoc, actualDoc);
    }

    protected abstract String getTopicName();

    protected abstract void send()
            throws IOException, IllegalAccessException, InstantiationException;

    protected abstract Document getExpectedDocument();

}
