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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.Stat.AVERAGE;
import static org.radarcns.sink.util.MongoConstants.Stat.INTERQUARTILE_RANGE;
import static org.radarcns.sink.util.MongoConstants.Stat.MAXIMUM;
import static org.radarcns.sink.util.MongoConstants.Stat.MINIMUM;
import static org.radarcns.sink.util.MongoConstants.Stat.QUARTILES;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.AVG;
import static org.radarcns.sink.util.RadarAvroConstants.COUNT;
import static org.radarcns.sink.util.RadarAvroConstants.IQR;
import static org.radarcns.sink.util.RadarAvroConstants.MAX;
import static org.radarcns.sink.util.RadarAvroConstants.MIN;
import static org.radarcns.sink.util.RadarAvroConstants.QUARTILE;
import static org.radarcns.sink.util.RadarAvroConstants.SUM;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.sink.mongodb.converter.DoubleCollectorConverter;
import org.radarcns.sink.util.Converter;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.Stat;
import org.radarcns.sink.util.UtilityTest;

public class DoubleCollectorConverterTest {

    private DoubleCollectorConverter converter;

    private Schema valueSchema;

    private static final Double MOCK_VALUE = 99.99;

    /** Initializer. */
    @Before
    public void setUp() {
        this.converter = new DoubleCollectorConverter();

        this.valueSchema = SchemaBuilder.struct().field(
            MIN, Schema.FLOAT64_SCHEMA).field(
            MAX, Schema.FLOAT64_SCHEMA).field(
            SUM, Schema.FLOAT64_SCHEMA).field(
            COUNT, Schema.FLOAT64_SCHEMA).field(
            AVG, Schema.FLOAT64_SCHEMA).field(
            QUARTILE, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            IQR, Schema.FLOAT64_SCHEMA).build();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {
        Long time = System.currentTimeMillis();
        String user = "user";
        String source = "source";

        Struct keyStruct = UtilityTest.getKeyStruct(user, source, time);

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueSchema, getValueStruct(), 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        assertTrue(document.get(MINIMUM.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(MINIMUM.getParam()), 0);

        assertTrue(document.get(MAXIMUM.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(MAXIMUM.getParam()), 0);

        assertTrue(document.get(Stat.SUM.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(Stat.SUM.getParam()), 0);

        assertTrue(document.get(Stat.COUNT.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(Stat.COUNT.getParam()), 0);

        assertTrue(document.get(AVERAGE.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(AVERAGE.getParam()), 0);

        assertTrue(document.get(QUARTILES.getParam()) instanceof List);
        assertEquals(Converter.extractQuartile(UtilityTest.getMockList(MOCK_VALUE)),
                document.get(QUARTILES.getParam()));

        assertTrue(document.get(INTERQUARTILE_RANGE.getParam()) instanceof Double);
        assertEquals(MOCK_VALUE, document.getDouble(INTERQUARTILE_RANGE.getParam()), 0);

        assertTrue(document.get(MongoConstants.START) instanceof Date);
        assertEquals(time, document.getDate(MongoConstants.START).getTime(), 0);

        assertTrue(document.get(MongoConstants.END) instanceof Date);
        assertEquals(time, document.getDate(MongoConstants.END).getTime(), 0);
    }

    private Struct getValueStruct() {
        Struct valueStruct = new Struct(valueSchema);

        valueStruct.put(MIN, MOCK_VALUE);
        valueStruct.put(MAX, MOCK_VALUE);
        valueStruct.put(SUM, MOCK_VALUE);
        valueStruct.put(COUNT, MOCK_VALUE);
        valueStruct.put(AVG, MOCK_VALUE);
        valueStruct.put(QUARTILE, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(IQR, MOCK_VALUE);

        return valueStruct;
    }

}
