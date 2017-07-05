package org.radarcns.sink.mongodb.converter;

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

import static org.radarcns.sink.util.KeyGenerator.intervalKeyToMongoKey;
import static org.radarcns.sink.util.KeyGenerator.toDateTime;
import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.sink.util.MongoConstants.TYPE;
import static org.radarcns.sink.util.MongoConstants.VALUE;
import static org.radarcns.sink.util.RadarAvroConstants.ANSWERS;
import static org.radarcns.sink.util.RadarAvroConstants.END_TIME;
import static org.radarcns.sink.util.RadarAvroConstants.NAME;
import static org.radarcns.sink.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.util.RadarAvroConstants.START_TIME;
import static org.radarcns.sink.util.RadarAvroConstants.USER_ID;
import static org.radarcns.sink.util.RadarAvroConstants.VERSION;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.TypeLabel;
import org.radarcns.sink.util.RadarAvroConstants;

/**
 * {@link RecordConverter} to convert a {@link Questionnaire} record to Bson Document.
 */
public class QuestionnaireConverter implements RecordConverter {

    //private static final Logger LOGGER = LoggerFactory.getLogger(QuestionnaireConverter.class);

    /**
     * Returns a {@code Collection<String>} reporting schema names supported by this converter.
     *      These names behaves as the key for selecting the suitable {@link RecordConverter} for
     *      a {@link SinkRecord}.
     *
     * @return a {@code Collection<String>} containing the supported Avro schema names
     */
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(MeasurementKey.class.getCanonicalName() + "-"
                + Questionnaire.class.getCanonicalName());
    }

    /**
     * Converts the given {@link SinkRecord} into a custom {@link Document}.
     *
     * @param sinkRecord {@link SinkRecord} to be converted
     * @return a {@link Document} representing the input {@link SinkRecord}
     */
    @Override
    public Document convert(SinkRecord sinkRecord) throws DataException {
        Struct key = (Struct) sinkRecord.key();
        Struct value = (Struct) sinkRecord.value();

        return new Document(ID, intervalKeyToMongoKey(key, value.getFloat64(START_TIME),
            value.getFloat64(END_TIME))).append(
            MongoConstants.USER, key.getString(USER_ID)).append(
            MongoConstants.SOURCE, key.getString(SOURCE_ID)).append(
            MongoConstants.NAME, value.getString(NAME)).append(
            MongoConstants.VERSION, value.getString(VERSION)).append(
            MongoConstants.ANSWERS, getAnswers(value.getArray(ANSWERS))).append(
            MongoConstants.START, toDateTime(value.getFloat64(START_TIME))).append(
            MongoConstants.END, toDateTime(value.getFloat64(END_TIME)));
    }

    private static List<Document> getAnswers(List<Object> input) {
        List<Document> answers = new LinkedList<>();

        for (Object item : input) {
            Struct record = (Struct) item;

            TypeLabel type = null;
            for (TypeLabel local : TypeLabel.values()) {
                if (record.getStruct(RadarAvroConstants.VALUE).get(local.getParam()) != null) {
                    type = local;
                    break;
                }
            }

            Document doc;
            switch (type) {
                case INT:
                    doc = new Document(TYPE, type.getParam()).append(
                        VALUE, record.getStruct(
                        RadarAvroConstants.VALUE).getInt32(type.getParam()));
                    break;
                case STRING:
                    doc = new Document(TYPE, type.getParam()).append(
                        VALUE, record.getStruct(
                        RadarAvroConstants.VALUE).getString(type.getParam()));
                    break;
                case DOUBLE:
                    doc = new Document(TYPE, type.getParam()).append(
                        VALUE, record.getStruct(
                        RadarAvroConstants.VALUE).getFloat64(type.getParam()));
                    break;
                default: throw new DataException(type + " cannot be converted");
            }
            doc.append(MongoConstants.START, toDateTime(record.getFloat64(START_TIME)));
            doc.append(MongoConstants.END, toDateTime(record.getFloat64(END_TIME)));

            answers.add(doc);
        }

        return answers;
    }
}
