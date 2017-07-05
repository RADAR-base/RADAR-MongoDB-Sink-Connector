package org.radarcns.sink.util;

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

import static org.radarcns.sink.util.RadarAvroConstants.END;
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;
import static org.radarcns.sink.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.util.RadarAvroConstants.START;
import static org.radarcns.sink.util.RadarAvroConstants.USER_ID;

import java.util.Date;
import org.apache.kafka.connect.data.Struct;

/**
 * Generic converter for MongoDB.
 */
public final class KeyGenerator {

    /**
     * Private constructor to prevent instantiation.
     */
    private KeyGenerator() {}

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
     * Return the concatenation of fields {@link RadarAvroConstants#USER_ID},
     *      {@link RadarAvroConstants#SOURCE_ID} and the conversion of {@code start} and {@code end}
     *      as timestamp separated by {@link RadarAvroConstants#SEPARATOR}
     *      as a {@link String}. As long as the two id fields are unique and there is no other value
     *      for the same time interval, then the result will be unique as well.
     *
     * @param key {@link Struct} representing the key of a Kafka message
     * @param start {@link Double} representing the start time related to the record
     * @param end {@link Double} representing the end time related to the record
     * @return converted key string
     *
     * @see #toDateTime(Object)
     */
    public static String intervalKeyToMongoKey(Struct key, Double start, Double end) {
        return key.getString(USER_ID).concat(SEPARATOR).concat(key.getString(SOURCE_ID)).concat(
            SEPARATOR).concat(Long.toString(toDateTime(start).getTime())).concat(
            SEPARATOR).concat(Long.toString(toDateTime(end).getTime()));
    }

    /**
     * Return the concatenation of fields {@link RadarAvroConstants#USER_ID},
     *      {@link RadarAvroConstants#SOURCE_ID}, {@link RadarAvroConstants#START} and
     *      {@link RadarAvroConstants#END} separated by {@link RadarAvroConstants#SEPARATOR}
     *      as a {@link String}. As long as the two id fields are unique and there is no other value
     *      for the same time interval, then the result will be unique as well.
     *
     * @param key {@link Struct} representing the key of a Kafka message
     * @return converted key string
     *
     * @see #toDateTime(Object)
     */
    public static String windowedKeyToMongoKey(Struct key) {
        return key.getString(USER_ID).concat(SEPARATOR).concat(key.getString(SOURCE_ID)).concat(
            SEPARATOR).concat(key.getInt64(START).toString()).concat(SEPARATOR).concat(
            key.getInt64(END).toString());
    }
}
