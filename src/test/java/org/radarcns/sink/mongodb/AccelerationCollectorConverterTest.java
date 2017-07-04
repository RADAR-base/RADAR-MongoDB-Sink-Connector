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
import static org.radarcns.sink.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.Stat.AVERAGE;
import static org.radarcns.sink.util.MongoConstants.Stat.INTERQUARTILE_RANGE;
import static org.radarcns.sink.util.MongoConstants.Stat.MAXIMUM;
import static org.radarcns.sink.util.MongoConstants.Stat.MINIMUM;
import static org.radarcns.sink.util.MongoConstants.Stat.QUARTILES;
import static org.radarcns.sink.util.MongoConstants.THIRD_QUARTILE;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.MongoConstants.X_LABEL;
import static org.radarcns.sink.util.MongoConstants.Y_LABEL;
import static org.radarcns.sink.util.MongoConstants.Z_LABEL;
import static org.radarcns.sink.util.RadarAvroConstants.AVG;
import static org.radarcns.sink.util.RadarAvroConstants.COUNT;
import static org.radarcns.sink.util.RadarAvroConstants.IQR;
import static org.radarcns.sink.util.RadarAvroConstants.MAX;
import static org.radarcns.sink.util.RadarAvroConstants.MIN;
import static org.radarcns.sink.util.RadarAvroConstants.QUARTILE;
import static org.radarcns.sink.util.RadarAvroConstants.SUM;

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
import org.radarcns.sink.util.Converter;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.Stat;
import org.radarcns.sink.util.UtilityTest;

/**
 * {@link AccelerationCollectorConverter} test case.
 */
public class AccelerationCollectorConverterTest {

    private AccelerationCollectorConverter converter;

    private Schema valueSchema;

    private static final Double MOCK_VALUE = 99.99;

    /**
     * Initializer.
     */
    @Before
    public void setUp() {
        this.converter = new AccelerationCollectorConverter();

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

        Struct keyStruct = UtilityTest.getKeyStruct(user, source, time);

        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(MIN, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(MAX, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(SUM, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(COUNT, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(AVG, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(QUARTILE, getMockQuartile());
        valueStruct.put(IQR, UtilityTest.getMockList(MOCK_VALUE));

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        Document valuesDocument = AccelerationCollectorConverter.sampleToDocument(
                UtilityTest.getMockList(MOCK_VALUE));

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

        Document actual = AccelerationCollectorConverter.sampleToDocument(
                UtilityTest.getMockList(MOCK_VALUE));

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
                X_LABEL, Converter.extractQuartile(UtilityTest.getMockList(MOCK_VALUE))).append(
                Y_LABEL, Converter.extractQuartile(UtilityTest.getMockList(MOCK_VALUE))).append(
                Z_LABEL, Converter.extractQuartile(UtilityTest.getMockList(MOCK_VALUE)));

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

    private List<List<Double>> getMockQuartile() {
        List<List<Double>> quartileValues = new LinkedList<>();
        quartileValues.add(UtilityTest.getMockList(MOCK_VALUE));
        quartileValues.add(UtilityTest.getMockList(MOCK_VALUE));
        quartileValues.add(UtilityTest.getMockList(MOCK_VALUE));

        return quartileValues;
    }

}
