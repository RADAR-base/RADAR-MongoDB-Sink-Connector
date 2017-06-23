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

import static org.radarcns.sink.util.Converter.extractQuartile;
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
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.util.Converter;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.Stat;
import org.radarcns.sink.util.RadarAvroConstants;
import org.radarcns.util.Utility;

/**
 * {@link RecordConverter} to convert a {@link DoubleArrayAggregator} record to Bson Document.
 */
public class AccelerationCollectorConverter implements RecordConverter {

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
                + DoubleArrayAggregator.class.getCanonicalName());
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

        return new Document(ID, Utility.intervalKeyToMongoKey(key)).append(
                USER, key.getString(USER_ID)).append(
                SOURCE, key.getString(SOURCE_ID)).append(
                Stat.MINIMUM.getParam(), sampleToDocument(value.getArray(MIN))).append(
                Stat.MAXIMUM.getParam(), sampleToDocument(value.getArray(MAX))).append(
                Stat.SUM.getParam(), sampleToDocument(value.getArray(SUM))).append(
                Stat.COUNT.getParam(), sampleToDocument(value.getArray(COUNT))).append(
                Stat.AVERAGE.getParam(), sampleToDocument(value.getArray(AVG))).append(
                Stat.QUARTILES.getParam(), quartileToDocument(value.getArray(QUARTILE))).append(
                Stat.INTERQUARTILE_RANGE.getParam(), sampleToDocument(value.getArray(IQR))).append(
                MongoConstants.START,
                    Converter.toDateTime(key.getInt64(RadarAvroConstants.START))).append(
                MongoConstants.END,
                    Converter.toDateTime(key.getInt64(RadarAvroConstants.END)));
    }

    /**
     * Converts the given {@code List<Double>} into a {@link Document} representing an acceleration
     *      sample with 3 axises. The first entry is associated to the
     *      {@link MongoConstants#X_LABEL} the second one to {@link MongoConstants#Y_LABEL} while
     *      the third one to {@link MongoConstants#Z_LABEL}.
     *
     * @param component {@code List<Double>} containing acceleration samples
     *
     * @return a {@link Document} containing values of the 3 axises
     */
    public static Document sampleToDocument(List<Double> component) {
        return new Document(
                MongoConstants.X_LABEL, component.get(0)).append(
                MongoConstants.Y_LABEL, component.get(1)).append(
                MongoConstants.Z_LABEL, component.get(2));
    }

    /**
     * Converts the given {@code List<List<Double>>} into a {@link Document} representing quartile
     *      of acceleration sample with 3 axises. The first entry is associated to the
     *      {@link MongoConstants#X_LABEL} the second one to {@link MongoConstants#Y_LABEL} while
     *      the third one to {@link MongoConstants#Z_LABEL}. Each entry is a list having as first
     *      {@link MongoConstants#FIRST_QUARTILE}, then {@link MongoConstants#SECOND_QUARTILE} and
     *      {@link MongoConstants#THIRD_QUARTILE}.
     *
     * @param list {@code List<List<Double>>} containing quartile for 3 acceleration axises
     *
     * @return a {@link Document} containing values of the 3 axises
     */
    public static Document quartileToDocument(List<List<Double>> list) {
        return new Document(
                MongoConstants.X_LABEL, extractQuartile(list.get(0))).append(
                MongoConstants.Y_LABEL, extractQuartile(list.get(1))).append(
                MongoConstants.Z_LABEL, extractQuartile(list.get(2)));
    }
}
