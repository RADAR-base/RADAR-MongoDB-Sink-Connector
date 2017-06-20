/*
 *  Copyright 2016 Kings College London and The Hyve
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

package org.radarcns.sink.mongodb.converter;

import static org.radarcns.sink.mongodb.util.MongoConstants.ID;
import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
import static org.radarcns.sink.mongodb.util.MongoConstants.USER;
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
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.Document;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.mongodb.util.MongoConstants;
import org.radarcns.sink.mongodb.util.MongoConstants.Stat;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;
import org.radarcns.util.Utility;

public class DoubleArrayCollectorConverter implements RecordConverter {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName());
    }

    @Override
    public Document convert(SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document(ID, Utility.intervalKeyToMongoKey(key)).append(
                USER, key.getString(USER_ID)).append(
                SOURCE, key.getString(SOURCE_ID)).append(
                Stat.MINIMUM.getParam(), new BsonDouble(value.getFloat64(MIN))).append(
                Stat.MAXIMUM.getParam(), new BsonDouble(value.getFloat64(MAX))).append(
                Stat.SUM.getParam(), new BsonDouble(value.getFloat64(SUM))).append(
                Stat.COUNT.getParam(), new BsonDouble(value.getFloat64(COUNT))).append(
                Stat.AVERAGE.getParam(), new BsonDouble(value.getFloat64(AVG))).append(
                Stat.QUARTILES.getParam(),
                        Utility.extractQuartile(value.getArray(QUARTILE))).append(
                Stat.INTERQUARTILE_RANGE.getParam(), new BsonDouble(value.getFloat64(IQR))).append(
                MongoConstants.START,
                        new BsonDateTime(key.getInt64(RadarAvroConstants.START))).append(
                MongoConstants.END, new BsonDateTime(key.getInt64(RadarAvroConstants.END)));
    }
}
