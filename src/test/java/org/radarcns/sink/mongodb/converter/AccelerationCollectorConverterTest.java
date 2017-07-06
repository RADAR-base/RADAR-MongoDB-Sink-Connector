package org.radarcns.sink.mongodb.converter;

/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http:www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.Stat;
import org.radarcns.sink.util.RadarUtility;
import org.radarcns.sink.util.UtilityTest;
import org.radarcns.sink.util.struct.AvroToStruct;

/**
 * {@link AccelerationCollectorConverter} test case.
 */
public class AccelerationCollectorConverterTest {

    private AccelerationCollectorConverter converter;

    private static final Double MOCK_VALUE = 99.99;
    private static final String USER_VALUE = "user";
    private static final String SOURCE_VALUE = "source";

    private Long time;

    /**
     * Initializer.
     */
    @Before
    public void setUp() {
        this.converter = new AccelerationCollectorConverter();
        this.time = System.currentTimeMillis();
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
        Struct keyStruct = UtilityTest.getKeyStruct(USER_VALUE, SOURCE_VALUE, time);
        Struct valueStruct = getValueStruct();

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueStruct.schema(), valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(USER_VALUE, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(SOURCE_VALUE, document.get(SOURCE));

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
                X_LABEL, RadarUtility.extractQuartile(UtilityTest.getMockList(MOCK_VALUE))).append(
                Y_LABEL, RadarUtility.extractQuartile(UtilityTest.getMockList(MOCK_VALUE))).append(
                Z_LABEL, RadarUtility.extractQuartile(UtilityTest.getMockList(MOCK_VALUE)));

        Document actual = AccelerationCollectorConverter.quartileToDocument(getMockQuartile());

        assertEquals(expected, actual);

        List<Document> documents;
        assertTrue(actual.get(X_LABEL) instanceof List);
        documents = getComponent(actual, X_LABEL);
        assertEquals(MOCK_VALUE, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);

        assertTrue(actual.get(Y_LABEL) instanceof List);
        documents = getComponent(actual, Y_LABEL);
        assertEquals(MOCK_VALUE, documents.get(0).getDouble(FIRST_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(1).getDouble(SECOND_QUARTILE), 0.0);
        assertEquals(MOCK_VALUE, documents.get(2).getDouble(THIRD_QUARTILE), 0.0);

        assertTrue(actual.get(Z_LABEL) instanceof List);
        documents = getComponent(actual, Z_LABEL);
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

    private Struct getValueStruct() {
        Struct valueStruct = new Struct(AvroToStruct.convertSchema(
                DoubleArrayAggregator.getClassSchema()));
        valueStruct.put(MIN, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(MAX, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(SUM, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(COUNT, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(AVG, UtilityTest.getMockList(MOCK_VALUE));
        valueStruct.put(QUARTILE, getMockQuartile());
        valueStruct.put(IQR, UtilityTest.getMockList(MOCK_VALUE));

        return valueStruct;
    }

    @SuppressWarnings("unchecked")
    private List<Document> getComponent(Document document, String label) {
        return (List<Document>) document.get(label);
    }

}
