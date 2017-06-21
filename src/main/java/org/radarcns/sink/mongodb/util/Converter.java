package org.radarcns.sink.mongodb.util;

/*
 * Copyright 2016 King's College London and The Hyve
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

import static org.radarcns.sink.mongodb.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.THIRD_QUARTILE;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SEPARATOR;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.USER_ID;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;

/**
 * Generic converter for MongoDB.
 */
public final class Converter {

    /**
     * Private constructor to prevent instantiation.
     */
    private Converter() {}

    /**
     * Try to convert input to {@link java.util.Date}.
     *
     * @param obj raw value
     * @return {@link java.util.Date} or null if unsuccessful
     */
    public static Date toDateTime(Object obj) {
        if (obj instanceof Long) {
            return new Date((Long)obj);
        } else if (obj instanceof Double) {
            return new Date((long)(1000d * (Double) obj));
        } else {
            return null;
        }
    }

    /**
     * Return a {@link String} that is the result of the concatenation of fields
     *      {@link RadarAvroConstants#USER_ID} and {@link RadarAvroConstants#SOURCE_ID} separated
     *      by {@link RadarAvroConstants#SEPARATOR}. As long as the two fields are unique, then the
     *      result will be unique as well.
     *
     * @param key {@link Struct} representing the key of a Kafka message
     * @return converted key string
     */
    public static String measurementKeyToMongoDbKey(Struct key) {
        return key.getString(USER_ID).concat(SEPARATOR).concat(key.getString(SOURCE_ID));
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
