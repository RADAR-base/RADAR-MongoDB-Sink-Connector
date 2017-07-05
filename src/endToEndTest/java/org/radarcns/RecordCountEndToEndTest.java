package org.radarcns;

/*
 * Copyright 2017 Kings College London and The Hyve
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
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;

import java.io.IOException;
import org.bson.Document;
import org.radarcns.application.ApplicationRecordCounts;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.util.KeyGenerator;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.topic.SensorTopic;
import org.radarcns.util.Sender;
import org.radarcns.util.SenderTestCase;

/**
 * MongoDB Sink connector e2e test. It streams a static generated {@link ApplicationRecordCounts}
 *      message into the data pipeline and then queries MongoDb to check that data has been
 *      correctly stored.
 */
public class RecordCountEndToEndTest extends SenderTestCase {

    private static final Integer MOCK_VALUE = 10;
    private static final double TIME = System.currentTimeMillis() / 1000d;

    @Override
    protected String getTopicName() {
        return "application_record_counts";
    }

    @Override
    protected void send()
            throws IOException, IllegalAccessException, InstantiationException {
        MeasurementKey key = new MeasurementKey(USER_ID_MOCK, SOURCE_ID_MOCK);

        ApplicationRecordCounts recordCounts = new ApplicationRecordCounts(TIME, TIME, MOCK_VALUE,
                MOCK_VALUE, MOCK_VALUE);

        SensorTopic<MeasurementKey, ApplicationRecordCounts> topic =
                new SensorTopic<>(getTopicName(), MeasurementKey.getClassSchema(),
                recordCounts.getSchema(), MeasurementKey.class, ApplicationRecordCounts.class);

        Sender<MeasurementKey, ApplicationRecordCounts> sender = new Sender<>(config, topic);

        sender.send(key, recordCounts);

        sender.close();
    }

    @Override
    protected Document getExpectedDocument() {
        return new Document(ID, USER_ID_MOCK.concat(SEPARATOR).concat(SOURCE_ID_MOCK)).append(
                USER, USER_ID_MOCK).append(SOURCE, SOURCE_ID_MOCK).append(
                MongoConstants.RECORDS_CACHED, MOCK_VALUE).append(
                MongoConstants.RECORDS_SENT, MOCK_VALUE).append(
                MongoConstants.RECORDS_UNSENT, MOCK_VALUE).append(
                TIMESTAMP, KeyGenerator.toDateTime(TIME));
    }
}
