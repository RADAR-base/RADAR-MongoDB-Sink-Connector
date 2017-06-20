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
import static org.radarcns.sink.mongodb.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.AVERAGE;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.INTERQUARTILE_RANGE;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.MAXIMUM;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.MINIMUM;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.QUARTILES;
import static org.radarcns.sink.mongodb.util.MongoConstants.THIRD_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.USER;
import static org.radarcns.sink.mongodb.util.MongoConstants.X_LABEL;
import static org.radarcns.sink.mongodb.util.MongoConstants.Y_LABEL;
import static org.radarcns.sink.mongodb.util.MongoConstants.Z_LABEL;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.AVG;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.COUNT;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.IQR;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.MAX;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.MIN;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.QUARTILE;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SUM;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.USER_ID;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.sink.mongodb.converter.AccelerationCollectorConverter;
import org.radarcns.sink.mongodb.util.Converter;
import org.radarcns.sink.mongodb.util.MongoConstants;
import org.radarcns.sink.mongodb.util.MongoConstants.Stat;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;

public class AccelerationCollectorConverterTest {

    private AccelerationCollectorConverter converter;

    private Schema keySchema;
    private Schema valueSchema;

    private static final Double MOCK_VALUE = 99.99;

    /**
     * Initializer.
     */
    @Before
    public void setUp() {
        this.converter = new AccelerationCollectorConverter();

        this.keySchema = SchemaBuilder.struct()
            .field(USER_ID, Schema.STRING_SCHEMA)
            .field(SOURCE_ID, Schema.STRING_SCHEMA)
            .field(RadarAvroConstants.START, Schema.INT64_SCHEMA)
            .field(RadarAvroConstants.END, Schema.INT64_SCHEMA).build();

        this.valueSchema = SchemaBuilder.struct().field(
            MIN, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            MAX, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            SUM, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            COUNT, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            AVG, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).field(
            QUARTILE, SchemaBuilder.array(
                SchemaBuilder.array(Schema.FLOAT64_SCHEMA))).field(
            IQR, SchemaBuilder.array(Schema.FLOAT64_SCHEMA)).build();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(WindowedKey.class.getCanonicalName() + "-"
                + DoubleArrayAggregator.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {
        Long time = System.currentTimeMillis();
        String user = "user";
        String source = "source";

        Struct keyStruct = new Struct(keySchema);
        keyStruct.put(USER_ID, user);
        keyStruct.put(SOURCE_ID, source);
        keyStruct.put(RadarAvroConstants.START, time);
        keyStruct.put(RadarAvroConstants.END, time);

        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(MIN, getMockList());
        valueStruct.put(MAX, getMockList());
        valueStruct.put(SUM, getMockList());
        valueStruct.put(COUNT, getMockList());
        valueStruct.put(AVG, getMockList());
        valueStruct.put(QUARTILE, getMockQuartile());
        valueStruct.put(IQR, getMockList());

        SinkRecord record = new SinkRecord("mine", 0, keySchema,
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        Document valuesDocument = AccelerationCollectorConverter.sampleToDocument(getMockList());

        assertTrue(document.get(MINIMUM.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(MINIMUM.getParam()));

        assertTrue(document.get(MAXIMUM.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(MAXIMUM.getParam()));

        assertTrue(document.get(Stat.SUM.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(Stat.SUM.getParam()));

        assertTrue(document.get(Stat.COUNT.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(Stat.COUNT.getParam()));

        assertTrue(document.get(AVERAGE.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(AVERAGE.getParam()));

        assertTrue(document.get(QUARTILES.getParam()) instanceof Document);
        assertEquals(AccelerationCollectorConverter.quartileToDocument(
                getMockQuartile()), document.get(QUARTILES.getParam()));

        assertTrue(document.get(INTERQUARTILE_RANGE.getParam()) instanceof Document);
        assertEquals(valuesDocument, document.get(INTERQUARTILE_RANGE.getParam()));

        assertTrue(document.get(MongoConstants.START) instanceof Date);
        assertEquals(time, document.getDate(MongoConstants.START).getTime(), 0);

        assertTrue(document.get(MongoConstants.END) instanceof Date);
        assertEquals(time, document.getDate(MongoConstants.END).getTime(), 0);
    }

    @Test
    public void sampleToDocument() {
        Document expected = new Document(
                X_LABEL, MOCK_VALUE).append(
                Y_LABEL, MOCK_VALUE).append(
                Z_LABEL, MOCK_VALUE);

        Document actual = AccelerationCollectorConverter.sampleToDocument(getMockList());

        assertEquals(expected, actual);

        assertTrue(actual.get(X_LABEL) instanceof Double);
        assertEquals(MOCK_VALUE, actual.getDouble(X_LABEL), 0);

        assertTrue(actual.get(Y_LABEL) instanceof Double);
        assertEquals(MOCK_VALUE, actual.getDouble(Y_LABEL), 0);

        assertTrue(actual.get(Z_LABEL) instanceof Double);
        assertEquals(MOCK_VALUE, actual.getDouble(Z_LABEL), 0);
    }

    @Test
    public void quartileToDocument() {
        Document expected = new Document(
                X_LABEL, Converter.extractQuartile(getMockList())).append(
                Y_LABEL, Converter.extractQuartile(getMockList())).append(
                Z_LABEL, Converter.extractQuartile(getMockList()));

        Document actual = AccelerationCollectorConverter.quartileToDocument(getMockQuartile());

        assertEquals(expected, actual);

        List<Document> documents;
        assertTrue(actual.get(X_LABEL) instanceof List);
        documents = (List<Document>) actual.get(X_LABEL);
        assertEquals(MOCK_VALUE, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);

        assertTrue(actual.get(Y_LABEL) instanceof List);
        documents = (List<Document>) actual.get(Y_LABEL);
        assertEquals(MOCK_VALUE, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);

        assertTrue(actual.get(Z_LABEL) instanceof List);
        documents = (List<Document>) actual.get(Z_LABEL);
        assertEquals(MOCK_VALUE, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);
    }

    private List<Double> getMockList() {
        List<Double> values = new LinkedList<>();
        values.add(MOCK_VALUE);
        values.add(MOCK_VALUE);
        values.add(MOCK_VALUE);

        return values;
    }

    private List<List<Double>> getMockQuartile() {
        List<List<Double>> quartileValues = new LinkedList<>();
        quartileValues.add(getMockList());
        quartileValues.add(getMockList());
        quartileValues.add(getMockList());

        return quartileValues;
    }

}
