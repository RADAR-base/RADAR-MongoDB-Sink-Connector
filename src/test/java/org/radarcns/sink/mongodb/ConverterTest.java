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
import static org.radarcns.sink.mongodb.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.THIRD_QUARTILE;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SEPARATOR;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.USER_ID;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.sink.mongodb.util.Converter;

public class ConverterTest {

    @Test
    public void testKey() {
        Schema keySchema = SchemaBuilder.struct().field(
                USER_ID, Schema.STRING_SCHEMA).field(
                SOURCE_ID, Schema.STRING_SCHEMA).build();

        Struct keyStruct = new Struct(keySchema);
        keyStruct.put(USER_ID, "user");
        keyStruct.put(SOURCE_ID, "source");

        assertEquals("user".concat(SEPARATOR).concat("source"),
                Converter.measurementKeyToMongoDbKey(keyStruct));
    }

    @Test
    public void testDate() {
        Long time = System.currentTimeMillis();
        Date expected = new Date(time);

        assertEquals(expected, Converter.toDateTime(time));

        assertEquals(expected, Converter.toDateTime(time.doubleValue() / 1000));
    }

    @Test
    public void testQuartile() {
        Double value = 99.99;

        List<Double> quartile = new LinkedList<>();
        quartile.add(value);
        quartile.add(value);
        quartile.add(value);

        List<Document> documents = Converter.extractQuartile(quartile);

        assertEquals(value, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(value, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(value, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);
    }
}
