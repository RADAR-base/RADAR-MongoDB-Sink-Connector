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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.serialization.GenericRecordConverter;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.util.struct.StructAnalyser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordConverter} to convert a {@link Questionnaire} record to Bson Document.
 */
public class QuestionnaireConverter implements RecordConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuestionnaireConverter.class);

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
        try {
            LOGGER.info(StructAnalyser.prettyAnalise(sinkRecord.keySchema()));
            LOGGER.info(StructAnalyser.prettyAnalise(sinkRecord.valueSchema()));
        } catch (IOException e) {
            LOGGER.error("SinkRecord cannot be analysed", e);
        }

        //TODO implement it

        return new GenericRecordConverter().convert(sinkRecord);
    }
}
