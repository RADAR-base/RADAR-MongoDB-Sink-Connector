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

import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.util.RadarAvroConstants.TIME_RECEIVED;
import static org.radarcns.sink.util.RadarAvroConstants.USER_ID;

import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.application.ApplicationRecordCounts;
import org.radarcns.key.MeasurementKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.util.Converter;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.RadarAvroConstants;

/**
 * {@link RecordConverter} to convert a {@link ApplicationRecordCounts} record to Bson Document.
 */
public class RecordCountConverter implements RecordConverter {

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
                + ApplicationRecordCounts.class.getCanonicalName());
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

        return new Document(ID, Converter.measurementKeyToMongoDbKey(key)).append(
                USER, key.getString(USER_ID)).append(
                SOURCE, key.getString(SOURCE_ID)).append(
                MongoConstants.RECORDS_CACHED,
                        value.getInt32(RadarAvroConstants.RECORDS_CACHED)).append(
                MongoConstants.RECORDS_SENT,
                        value.getInt32(RadarAvroConstants.RECORDS_SENT)).append(
                MongoConstants.RECORDS_UNSENT,
                        value.getInt32(RadarAvroConstants.RECORDS_UNSENT)).append(
                TIMESTAMP, Converter.toDateTime(value.get(TIME_RECEIVED)));
    }


}
