package org.radarcns.sink.mongodb.converter;

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

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.radarcns.questionnaire.QuestionnaireType.PHQ8;
import static org.radarcns.sink.util.KeyGenerator.toDateTime;
import static org.radarcns.sink.util.MongoConstants.ANSWERS;
import static org.radarcns.sink.util.MongoConstants.END;
import static org.radarcns.sink.util.MongoConstants.NAME;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.START;
import static org.radarcns.sink.util.MongoConstants.TYPE;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.MongoConstants.VALUE;
import static org.radarcns.sink.util.MongoConstants.VERSION;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.questionnaire.QuestionnaireType;
import org.radarcns.sink.util.MongoConstants.TypeLabel;
import org.radarcns.sink.util.RadarAvroConstants;
import org.radarcns.sink.util.UtilityTest;
import org.radarcns.sink.util.struct.AvroToStruct;

/**
 * {@link RecordCountConverter} test case.
 */
public class QuestionnaireConverterTest {

    private QuestionnaireConverter converter;

    private Long time = System.currentTimeMillis();
    private int answerSize = 2;
    private String user ;
    private String source;
    private String subjectAnswer;
    private String version;
    private QuestionnaireType type;

    /** Initializer. */
    @Before
    public void setUp() {
        this.converter = new QuestionnaireConverter();

        this.time = System.currentTimeMillis();
        this.answerSize = 2;

        this.user = "user";
        this.source = "source";
        this.subjectAnswer = "I am ok";
        this.type = PHQ8;
        this.version = "v1.0";
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(MeasurementKey.class.getCanonicalName() + "-"
                + Questionnaire.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {
        Struct keyStruct = UtilityTest.getKeyStruct(user, source);

        Struct valueStruct = getValueStruct();

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueStruct.schema(), valueStruct, 0);

        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        assertTrue(document.get(NAME) instanceof String);
        assertEquals(type.name(), document.get(NAME));

        assertTrue(document.get(VERSION) instanceof String);
        assertEquals(version, document.get(VERSION));

        assertTrue(document.get(ANSWERS) instanceof LinkedList);
        assertEquals(expectedAnswers(), document.get(ANSWERS));

        assertTrue(document.get(START) instanceof Date);
        assertEquals(time, document.getDate(START).getTime(), 0);

        assertTrue(document.get(END) instanceof Date);
        assertEquals(time, document.getDate(END).getTime(), 0);
    }

    private List<Struct> getMockAnswer() {
        Schema answerSchema = AvroToStruct.convertSchema(Questionnaire.getClassSchema()).field(
                RadarAvroConstants.ANSWERS).schema().valueSchema();

        Schema valueSchema = answerSchema.field(RadarAvroConstants.VALUE).schema();

        List<Struct> list = new LinkedList<>();

        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(TypeLabel.STRING.getParam(), subjectAnswer);

        Struct answerStruct = new Struct(answerSchema);
        answerStruct.put(RadarAvroConstants.VALUE, valueStruct);
        answerStruct.put(RadarAvroConstants.START_TIME, time.doubleValue() / 1000d);
        answerStruct.put(RadarAvroConstants.END_TIME, time.doubleValue() / 1000d);

        for (int i = 0; i < answerSize; i++) {
            list.add(answerStruct);
        }

        return list;
    }

    private List<Document> expectedAnswers() {
        List<Document> list = new LinkedList<>();

        Document doc = new Document(TYPE, TypeLabel.STRING.getParam()).append(
                        VALUE, subjectAnswer).append(
                        START, toDateTime(time)).append(
                        END, toDateTime(time));

        for (int i = 0; i < answerSize; i++) {
            list.add(doc);
        }

        return list;
    }

    private Struct getValueStruct() {
        Struct valueStruct = new Struct(AvroToStruct.convertSchema(Questionnaire.getClassSchema()));
        valueStruct.put(RadarAvroConstants.NAME, type.name());
        valueStruct.put(RadarAvroConstants.VERSION, version);
        valueStruct.put(RadarAvroConstants.ANSWERS, getMockAnswer());
        valueStruct.put(RadarAvroConstants.START_TIME, time.doubleValue() / 1000d);
        valueStruct.put(RadarAvroConstants.END_TIME, time.doubleValue() / 1000d);

        return valueStruct;
    }
}
