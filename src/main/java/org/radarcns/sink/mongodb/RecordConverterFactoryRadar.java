package org.radarcns.sink.mongodb;

/*
 * Copyright 2016 King's College London and The Hyve
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

import java.util.ArrayList;
import java.util.List;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.sink.mongodb.converter.AccelerationCollectorConverter;
import org.radarcns.sink.mongodb.converter.DoubleCollectorConverter;
import org.radarcns.sink.mongodb.converter.RecordCountConverter;
import org.radarcns.sink.mongodb.converter.ServerStatusConverter;
import org.radarcns.sink.mongodb.converter.UptimeStatusConverter;

/**
 * Overrides {@link RecordConverterFactory} for adding customized {@link RecordConverter} to the
 *      ClassPath.
 */
public class RecordConverterFactoryRadar extends RecordConverterFactory {

    /**
     * Overrides {@link RecordConverterFactory#genericConverters()} to append custom
     *      {@link RecordConverter} class to {@link RecordConverterFactory}.
     *
     * @return {@code List<RecordConverter>} of available {@link RecordConverter}
     */
    @Override
    protected List<RecordConverter> genericConverters() {
        List<RecordConverter> recordConverters = new ArrayList<RecordConverter>();
        recordConverters.addAll(super.genericConverters());
        recordConverters.add(new AccelerationCollectorConverter());
        recordConverters.add(new DoubleCollectorConverter());
        recordConverters.add(new RecordCountConverter());
        recordConverters.add(new ServerStatusConverter());
        recordConverters.add(new UptimeStatusConverter());
        return recordConverters;
    }

}
