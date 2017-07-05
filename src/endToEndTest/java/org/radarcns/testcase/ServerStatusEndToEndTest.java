package org.radarcns.testcase;

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
import static org.radarcns.sink.util.MongoConstants.CLIENT_IP;
import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;

import java.io.IOException;
import org.bson.Document;
import org.radarcns.application.ApplicationServerStatus;
import org.radarcns.application.ServerStatus;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.topic.SensorTopic;
import org.radarcns.util.Sender;
import org.radarcns.util.SenderTestCase;

/**
 * MongoDB Sink connector e2e test. It streams a static generated {@link ApplicationServerStatus}
 *      message into the data pipeline and then queries MongoDb to check that data has been
 *      correctly stored.
 */
public class ServerStatusEndToEndTest extends SenderTestCase {

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String IP_ADDRESS = "127.0.0.1";
    private static final ServerStatus STATUS = ServerStatus.CONNECTED;
    private static final double TIME = System.currentTimeMillis() / 1000d;

    @Override
    public String getTopicName() {
        return "application_server_status";
    }

    @Override
    public void send()
            throws IOException, IllegalAccessException, InstantiationException {
        MeasurementKey key = new MeasurementKey(USER_ID_MOCK, SOURCE_ID_MOCK);

        ApplicationServerStatus status = new ApplicationServerStatus(TIME, TIME, STATUS,
                IP_ADDRESS);

        SensorTopic<MeasurementKey, ApplicationServerStatus> topic =
                new SensorTopic<>(getTopicName(), MeasurementKey.getClassSchema(),
                    status.getSchema(), MeasurementKey.class, ApplicationServerStatus.class);

        Sender<MeasurementKey, ApplicationServerStatus> sender = new Sender<>(config, topic);

        sender.send(key, status);

        sender.close();
    }

    @Override
    public Document getExpectedDocument() {
        return new Document(ID, USER_ID_MOCK.concat(SEPARATOR).concat(SOURCE_ID_MOCK)).append(
                USER, USER_ID_MOCK).append(
                SOURCE, SOURCE_ID_MOCK).append(
                MongoConstants.SERVER_STATUS, STATUS.name()).append(
                CLIENT_IP, IP_ADDRESS).append(
                TIMESTAMP, toDateTime(TIME));
    }
}
