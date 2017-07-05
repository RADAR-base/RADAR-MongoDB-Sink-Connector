package org.radarcns.sink.util;

import static org.radarcns.sink.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.THIRD_QUARTILE;

import java.util.Arrays;
import java.util.List;
import org.bson.Document;

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

public final class RadarUtility {

    private RadarUtility() {
        //Static class
    }

    /**
     * Convert a {@code List<Double>} to a {@code List<Document>} representing quartile. The first
     *      entry has {@link MongoConstants#FIRST_QUARTILE} as key, the second
     *      {@link MongoConstants#SECOND_QUARTILE} while the third ha
     *      {@link MongoConstants#THIRD_QUARTILE}.
     *
     * @param component input list to convert
     * @return {@code List<Document>} containing values for the three quartiles
     */
    public static List<Document> extractQuartile(List<Double> component) {
        return Arrays.asList(new Document[]{
            new Document(FIRST_QUARTILE, component.get(0)),
            new Document(SECOND_QUARTILE, component.get(1)),
            new Document(THIRD_QUARTILE, component.get(2))});
    }
}
