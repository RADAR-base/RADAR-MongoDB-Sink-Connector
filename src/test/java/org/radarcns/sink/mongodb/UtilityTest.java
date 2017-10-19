package org.radarcns.sink.mongodb;

import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.USER_ID;

import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;

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
public final class UtilityTest {

    private UtilityTest() {}

    /**
     * Returns {@link Schema} representing a {@link org.radarcns.key.WindowedKey}.
     */
    public static Schema getWindowedKeySchema() {
        return SchemaBuilder.struct().field(
            USER_ID, Schema.STRING_SCHEMA).field(
            SOURCE_ID, Schema.STRING_SCHEMA).field(
            RadarAvroConstants.START, Schema.INT64_SCHEMA).field(
            RadarAvroConstants.END, Schema.INT64_SCHEMA).build();
    }

    /**
     * Returns {@link Schema} representing a {@link org.radarcns.key.MeasurementKey}.
     */
    public static Schema getMeasuramentKey() {
        return SchemaBuilder.struct().field(
            USER_ID, Schema.STRING_SCHEMA).field(
            SOURCE_ID, Schema.STRING_SCHEMA).build();
    }

    /**
     * Returns {@link Struct} representing a {@link org.radarcns.key.MeasurementKey}.
     */
    public static Struct getKeyStruct(String user, String source) {
        return getKeyStruct(user, source, null);
    }

    /**
     * Returns {@link Struct} representing {@link org.radarcns.key.MeasurementKey} if {@code time}
     *      is null, otherwise the result represents a {@link org.radarcns.key.WindowedKey}.
     */
    public static Struct getKeyStruct(String user, String source, Long time) {
        Schema schema = time == null ? getMeasuramentKey() : getWindowedKeySchema();

        Struct keyStruct = new Struct(schema);

        keyStruct.put(USER_ID, user);
        keyStruct.put(SOURCE_ID, source);

        if (time != null) {
            keyStruct.put(RadarAvroConstants.START, time);
            keyStruct.put(RadarAvroConstants.END, time);
        }

        return keyStruct;
    }

    /**
     * Returns a {@code List<Double>} listing three times the input {@code mockValue}.
     */
    public static List<Double> getMockList(Double mockValue) {
        List<Double> values = new LinkedList<>();
        values.add(mockValue);
        values.add(mockValue);
        values.add(mockValue);

        return values;
    }

}
