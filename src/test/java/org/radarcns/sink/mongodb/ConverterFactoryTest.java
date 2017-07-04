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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.serialization.GenericRecordConverter;
import org.radarcns.serialization.RecordConverter;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

/**
 * {@link RecordConverterFactoryRadar} test case.
 */
public class ConverterFactoryTest {

    private RecordConverterFactoryRadar factory;

    private static final String PACKAGE = "org.radarcns.sink.mongodb.converter";

    @Before
    public void setUp() {
        this.factory = new RecordConverterFactoryRadar();
    }

    @Test
    public void converterFactoryTest() {
        Reflections reflections = new Reflections(PACKAGE,
                new SubTypesScanner(false));
        Set<Class<? extends Object>> expectedConverters = reflections.getSubTypesOf(Object.class);
        expectedConverters.add(GenericRecordConverter.class);

        assertEquals(expectedConverters.size(), factory.genericConverters().size());

        for (RecordConverter converters : factory.genericConverters()) {
            assertTrue(asList(converters.getClass().getInterfaces()).contains(
                    RecordConverter.class));
            expectedConverters.removeAll(asList(converters.getClass()));
        }

        assertTrue(expectedConverters.isEmpty());
    }

}
