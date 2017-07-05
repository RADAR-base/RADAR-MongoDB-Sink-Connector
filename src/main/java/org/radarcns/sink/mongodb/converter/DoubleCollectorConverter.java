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
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.AVG;
import static org.radarcns.sink.util.RadarAvroConstants.COUNT;
import static org.radarcns.sink.util.RadarAvroConstants.IQR;
import static org.radarcns.sink.util.RadarAvroConstants.MAX;
import static org.radarcns.sink.util.RadarAvroConstants.MIN;
import static org.radarcns.sink.util.RadarAvroConstants.QUARTILE;
import static org.radarcns.sink.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.util.RadarAvroConstants.SUM;
import static org.radarcns.sink.util.RadarAvroConstants.USER_ID;

import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.util.KeyGenerator;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.Stat;
import org.radarcns.sink.util.RadarAvroConstants;
import org.radarcns.sink.util.RadarUtility;

/**
 * {@link RecordConverter} to convert a {@link DoubleAggregator} record to Bson Document.
 */
public class DoubleCollectorConverter implements RecordConverter {

    /**
     * Returns a {@code Collection<String>} reporting schema names supported by this converter.
     *      These names behaves as the key for selecting the suitable {@link RecordConverter} for
     *      a {@link SinkRecord}.
     *
     * @return a {@code Collection<String>} containing the supported Avro schema names
     */
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName());
    }

    /**
     * Converts the given {@link SinkRecord} into a custom {@link Document}.
     *
     * @param sinkRecord {@link SinkRecord} to be converted
     * @return a {@link Document} representing the input {@link SinkRecord}
     */
    @Override
    public Document convert(SinkRecord sinkRecord) {
        Struct key = (Struct) sinkRecord.key();
        Struct value = (Struct) sinkRecord.value();

        return new Document(ID, KeyGenerator.windowedKeyToMongoKey(key)).append(
                USER, key.getString(USER_ID)).append(
                SOURCE, key.getString(SOURCE_ID)).append(
                Stat.MINIMUM.getParam(), value.getFloat64(MIN)).append(
                Stat.MAXIMUM.getParam(), value.getFloat64(MAX)).append(
                Stat.SUM.getParam(), value.getFloat64(SUM)).append(
                Stat.COUNT.getParam(), value.getFloat64(COUNT)).append(
                Stat.AVERAGE.getParam(), value.getFloat64(AVG)).append(
                Stat.QUARTILES.getParam(),
                    RadarUtility.extractQuartile(value.getArray(QUARTILE))).append(
                Stat.INTERQUARTILE_RANGE.getParam(), value.getFloat64(IQR)).append(
                MongoConstants.START,
                    KeyGenerator.toDateTime(key.getInt64(RadarAvroConstants.START))).append(
                MongoConstants.END, KeyGenerator.toDateTime(key.getInt64(RadarAvroConstants.END)));
    }
}
