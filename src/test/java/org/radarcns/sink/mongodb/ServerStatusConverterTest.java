package org.radarcns.sink.mongodb;

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
import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
import static org.radarcns.sink.mongodb.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.mongodb.util.MongoConstants.USER;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.TIME_RECEIVED;

import java.util.Collection;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.application.ServerStatus;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.mongodb.converter.ServerStatusConverter;
import org.radarcns.sink.mongodb.util.MongoConstants;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;

public class ServerStatusConverterTest {

    private ServerStatusConverter converter;
    private static final String STATUS = "CONNECTED";

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String IP_ADDRESS = "127.0.0.1";

    @Before
    public void setUp() {
        this.converter = new ServerStatusConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(MeasurementKey.class.getCanonicalName() + "-"
                + ServerStatus.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {
        Long time = System.currentTimeMillis();
        String user = "user";
        String source = "source";

        String timeField = "time";

        Struct keyStruct = UtilityTest.getKeyStruct(user, source);

        Schema valueSchema = SchemaBuilder.struct().field(
                timeField, Schema.FLOAT64_SCHEMA).field(
                TIME_RECEIVED, Schema.FLOAT64_SCHEMA).field(
                RadarAvroConstants.SERVER_STATUS, Schema.STRING_SCHEMA).field(
                RadarAvroConstants.IP_ADDRESS, Schema.STRING_SCHEMA).build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(timeField, time.doubleValue() / 1000d);
        valueStruct.put(TIME_RECEIVED, time.doubleValue() / 1000d);
        valueStruct.put(RadarAvroConstants.SERVER_STATUS, STATUS);
        valueStruct.put(RadarAvroConstants.IP_ADDRESS, IP_ADDRESS);

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueSchema, valueStruct, 0);

        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        assertTrue(document.get(MongoConstants.SERVER_STATUS) instanceof String);
        assertEquals(STATUS, document.get(MongoConstants.SERVER_STATUS));

        assertTrue(document.get(MongoConstants.CLIENT_IP) instanceof String);
        assertEquals(IP_ADDRESS, document.get(MongoConstants.CLIENT_IP));

        assertTrue(document.get(TIMESTAMP) instanceof Date);
        assertEquals(time, document.getDate(TIMESTAMP).getTime(), 0);

        assertNull(document.get(timeField));
    }
}
