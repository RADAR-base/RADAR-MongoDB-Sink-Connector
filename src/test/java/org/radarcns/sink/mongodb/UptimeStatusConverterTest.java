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
import static org.radarcns.sink.util.MongoConstants.APPLICATION_UPTIME;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.RadarAvroConstants.TIME_RECEIVED;

import java.util.Collection;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.application.ApplicationUptime;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.mongodb.converter.UptimeStatusConverter;
import org.radarcns.sink.util.RadarAvroConstants;
import org.radarcns.sink.util.UtilityTest;

public class UptimeStatusConverterTest {

    private UptimeStatusConverter converter;
    private static final Double UPTIME = 1000.00;

    @Before
    public void setUp() {
        this.converter = new UptimeStatusConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(MeasurementKey.class.getCanonicalName() + "-"
                + ApplicationUptime.class.getCanonicalName(), values.toArray()[0]);
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
                RadarAvroConstants.UPTIME, Schema.FLOAT64_SCHEMA).build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(timeField, time.doubleValue() / 1000d);
        valueStruct.put(TIME_RECEIVED, time.doubleValue() / 1000d);
        valueStruct.put(RadarAvroConstants.UPTIME, UPTIME);

        SinkRecord record = new SinkRecord("mine", 0, keyStruct.schema(),
                keyStruct, valueSchema, valueStruct, 0);

        Document document = this.converter.convert(record);

        assertNotNull(document);

        assertTrue(document.get(USER) instanceof String);
        assertEquals(user, document.get(USER));

        assertTrue(document.get(SOURCE) instanceof String);
        assertEquals(source, document.get(SOURCE));

        assertTrue(document.get(APPLICATION_UPTIME) instanceof Double);
        assertEquals(UPTIME, document.getDouble(APPLICATION_UPTIME), 0);

        assertTrue(document.get(TIMESTAMP) instanceof Date);
        assertEquals(time, document.getDate(TIMESTAMP).getTime(), 0);

        assertNull(document.get(timeField));
    }
}
