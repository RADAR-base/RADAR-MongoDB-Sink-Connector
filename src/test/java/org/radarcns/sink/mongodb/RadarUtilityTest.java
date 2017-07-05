package org.radarcns.sink.mongodb;

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

import static org.junit.Assert.assertEquals;
import static org.radarcns.sink.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.THIRD_QUARTILE;

import java.util.LinkedList;
import java.util.List;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.sink.util.RadarUtility;

/**
 * {@link RadarUtility} test case.
 */
public class RadarUtilityTest {

    @Test
    public void testQuartile() {
        Double value = 99.99;

        List<Double> quartile = new LinkedList<>();
        quartile.add(value);
        quartile.add(value);
        quartile.add(value);

        List<Document> documents = RadarUtility.extractQuartile(quartile);

        assertEquals(value, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(value, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(value, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);
    }
}
