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
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.sink.mongodb.util.MongoConstants;
import org.radarcns.sink.mongodb.util.MongoConstants.Stat;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;
import org.radarcns.util.Utility;

public class AccelerationCollectorConverter implements RecordConverter {

    public static final String X_LABEL = "x";
    public static final String Y_LABEL = "y";
    public static final String Z_LABEL = "z";

    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(WindowedKey.class.getCanonicalName() + "-"
                + DoubleArrayAggregator.class.getCanonicalName());
    }

    @Override
    public Document convert(SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document(ID, Utility.intervalKeyToMongoKey(key)).append(
                USER, key.getString(USER_ID)).append(
                SOURCE, key.getString(SOURCE_ID)).append(
                Stat.MINIMUM.getParam(), accCompToDoc(value.getArray(MIN))).append(
                Stat.MAXIMUM.getParam(), accCompToDoc(value.getArray(MAX))).append(
                Stat.SUM.getParam(), accCompToDoc(value.getArray(SUM))).append(
                Stat.COUNT.getParam(), accCompToDoc(value.getArray(COUNT))).append(
                Stat.AVERAGE.getParam(), accCompToDoc(value.getArray(AVG))).append(
                Stat.QUARTILES.getParam(), accQuartileToDoc(value.getArray(QUARTILE))).append(
                Stat.INTERQUARTILE_RANGE.getParam(), accCompToDoc(value.getArray(IQR))).append(
                MongoConstants.START,
                        new BsonDateTime(key.getInt64(RadarAvroConstants.START))).append(
                MongoConstants.END,
                        new BsonDateTime(key.getInt64(RadarAvroConstants.END)));
    }

    private static Document accCompToDoc(List<Double> component) {
        return new Document(X_LABEL, component.get(0)).append(
                Y_LABEL, component.get(1)).append(
                Z_LABEL, component.get(2));
    }

    private static Document accQuartileToDoc(List<List<Double>> list) {
        return new Document(X_LABEL, Utility.extractQuartile(list.get(0))).append(
                Y_LABEL, Utility.extractQuartile(list.get(1))).append(
                Z_LABEL, Utility.extractQuartile(list.get(2)));
    }
}
