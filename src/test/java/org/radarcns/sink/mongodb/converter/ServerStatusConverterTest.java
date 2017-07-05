package org.radarcns.sink.mongodb.converter;

/*
 * Copyright 2017 King's College London and The Hyve
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.TIME_RECEIVED;

import java.util.Collection;
import java.util.Date;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.application.ApplicationServerStatus;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.RadarAvroConstants;
import org.radarcns.sink.util.UtilityTest;
import org.radarcns.sink.util.struct.AvroToStruct;

/**
 * {@link ServerStatusConverter} test case.
 */
public class ServerStatusConverterTest {

    private ServerStatusConverter converter;

    private static final String STATUS = "CONNECTED";
    private static final String USER_VALUE = "user";
    private static final String SOURCE_VALUE = "source";
    private static final String TIME_FIELD = "time";

    private Long time;

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String IP_ADDRESS = "127.0.0.1";

    /** Initializer. */
    @Before
    public void setUp() {
        this.converter = new ServerStatusConverter();
        this.time = System.currentTimeMillis();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(MeasurementKey.class.getCanonicalName() + "-"
                + ApplicationServerStatus.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {
        Struct keyStruct = UtilityTest.getKeyStruct(USER_VALUE, SOURCE_VALUE);
        Struct valueStruct = getStructValue();

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueStruct.schema(), valueStruct, 0);

        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(USER_VALUE, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(SOURCE_VALUE, document.get(SOURCE));

        assertTrue(document.get(MongoConstants.SERVER_STATUS) instanceof String);
        assertEquals(STATUS, document.get(MongoConstants.SERVER_STATUS));

        assertTrue(document.get(MongoConstants.CLIENT_IP) instanceof String);
        assertEquals(IP_ADDRESS, document.get(MongoConstants.CLIENT_IP));

        assertTrue(document.get(TIMESTAMP) instanceof Date);
        assertEquals(time, document.getDate(TIMESTAMP).getTime(), 0);

        assertNull(document.get(TIME_FIELD));
    }

    private Struct getStructValue() {
        org.apache.avro.Schema valueSchema = ApplicationServerStatus.getClassSchema();

        Struct valueStruct = new Struct(AvroToStruct.convertSchema(valueSchema));

        valueStruct.put(TIME_FIELD, time.doubleValue() / 1000d);
        valueStruct.put(TIME_RECEIVED, time.doubleValue() / 1000d);
        valueStruct.put(RadarAvroConstants.SERVER_STATUS, STATUS);
        valueStruct.put(RadarAvroConstants.IP_ADDRESS, IP_ADDRESS);

        return valueStruct;
    }
}
