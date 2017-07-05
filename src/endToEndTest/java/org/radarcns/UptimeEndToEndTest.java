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

import static org.radarcns.sink.util.KeyGenerator.toDateTime;
import static org.radarcns.sink.util.MongoConstants.APPLICATION_UPTIME;
import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;

import java.io.IOException;
import org.bson.Document;
import org.radarcns.application.ApplicationServerStatus;
import org.radarcns.application.ApplicationUptime;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.SensorTopic;
import org.radarcns.util.Sender;
import org.radarcns.util.SenderTestCase;

/**
 * MongoDB Sink connector e2e test. It streams a static generated {@link ApplicationServerStatus}
 *      message into the data pipeline and then queries MongoDb to check that data has been
 *      correctly stored.
 */
public class UptimeEndToEndTest extends SenderTestCase {

    private static final Double MOCK_VALUE = 10d;
    private static final double TIME = System.currentTimeMillis() / 1000d;

    @Override
    protected String getTopicName() {
        return "application_uptime";
    }

    @Override
    protected void send()
            throws IOException, IllegalAccessException, InstantiationException {
        MeasurementKey key = new MeasurementKey(USER_ID_MOCK, SOURCE_ID_MOCK);

        ApplicationUptime uptime = new ApplicationUptime(TIME, TIME, MOCK_VALUE);

        SensorTopic<MeasurementKey, ApplicationUptime> topic =
                new SensorTopic<>(getTopicName(), MeasurementKey.getClassSchema(),
                    uptime.getSchema(), MeasurementKey.class, ApplicationUptime.class);

        Sender<MeasurementKey, ApplicationUptime> sender = new Sender<>(config, topic);

        sender.send(key, uptime);

        sender.close();
    }

    @Override
    protected Document getExpectedDocument() {
        return new Document(ID, USER_ID_MOCK.concat(SEPARATOR).concat(SOURCE_ID_MOCK)).append(
            USER, USER_ID_MOCK).append(
            SOURCE, SOURCE_ID_MOCK).append(
            APPLICATION_UPTIME, MOCK_VALUE).append(
            TIMESTAMP, toDateTime(TIME));
    }
}
