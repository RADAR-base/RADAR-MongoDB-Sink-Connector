package org.radarcns.sink.mongodb;

import static org.junit.Assert.assertEquals;
import static org.radarcns.sink.util.RadarAvroConstants.END;
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;
import static org.radarcns.sink.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.util.RadarAvroConstants.START;
import static org.radarcns.sink.util.RadarAvroConstants.USER_ID;

import java.util.Date;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.radarcns.sink.util.KeyGenerator;
import org.radarcns.sink.util.UtilityTest;

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

/**
 * {@link KeyGenerator} test case.
 */
public class KeyGeneratorTest {

    @Test
    public void testMeasurementKey() {
        String user = "user";
        String source = "source";

        Struct keyStruct = new Struct(UtilityTest.getMeasuramentKey());
        keyStruct.put(USER_ID, user);
        keyStruct.put(SOURCE_ID, source);

        assertEquals(user.concat(SEPARATOR).concat(source),
                KeyGenerator.measurementKeyToMongoDbKey(keyStruct));
    }

    @Test
    public void testWindowedKey() {
        String user = "user";
        String source = "source";
        long time = System.currentTimeMillis();

        Struct keyStruct = new Struct(UtilityTest.getWindowedKeySchema());
        keyStruct.put(USER_ID, user);
        keyStruct.put(SOURCE_ID, source);
        keyStruct.put(START, time);
        keyStruct.put(END, time);

        String expected = keyStruct.getString(USER_ID).concat(SEPARATOR).concat(
                keyStruct.getString(SOURCE_ID)).concat(SEPARATOR).concat(
                keyStruct.getInt64(START).toString()).concat(SEPARATOR).concat(
                keyStruct.getInt64(END).toString());

        assertEquals(expected,KeyGenerator.windowedKeyToMongoKey(keyStruct));
    }

    @Test
    public void testIntervalKey() {
        String user = "user";
        String source = "source";
        Long time = System.currentTimeMillis();
        Double timestamp = time.doubleValue() / 1000d;

        Struct keyStruct = new Struct(UtilityTest.getMeasuramentKey());
        keyStruct.put(USER_ID, user);
        keyStruct.put(SOURCE_ID, source);

        String expected = keyStruct.getString(USER_ID).concat(SEPARATOR).concat(
                keyStruct.getString(SOURCE_ID)).concat(SEPARATOR).concat(time.toString()).concat(
                SEPARATOR).concat(time.toString());

        assertEquals(expected, KeyGenerator.intervalKeyToMongoKey(keyStruct, timestamp, timestamp));
    }

    @Test
    public void testDate() {
        Long time = System.currentTimeMillis();
        Date expected = new Date(time);

        assertEquals(expected, KeyGenerator.toDateTime(time));

        assertEquals(expected, KeyGenerator.toDateTime(time.doubleValue() / 1000));
    }

}
