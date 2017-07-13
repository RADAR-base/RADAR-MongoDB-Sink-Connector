package org.radarcns.sink.util;

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

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Set of constants used to extract data from {@link SinkRecord}.
 */
public final class RadarAvroConstants {

    public static final String SEPARATOR = "-";

    public static final String USER_ID = "userId";
    public static final String SOURCE_ID = "sourceId";

    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String SUM = "sum";
    public static final String COUNT = "count";
    public static final String AVG = "avg";
    public static final String QUARTILE = "quartile";
    public static final String IQR = "iqr";

    public static final String START = "start";
    public static final String END = "end";

    public static final String RECORDS_CACHED = "recordsCached";
    public static final String RECORDS_SENT = "recordsSent";
    public static final String RECORDS_UNSENT = "recordsUnsent";

    public static final String SERVER_STATUS = "serverStatus";
    public static final String IP_ADDRESS = "ipAddress";

    public static final String UPTIME = "uptime";

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String ANSWERS = "answers";
    public static final String VALUE = "value";

    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";

    public static final String TIME = "time";
    public static final String TIME_COMPLETED = "timeCompleted";
    public static final String TIME_RECEIVED = "timeReceived";

    /**
     * Private constructor to prevent instantiation.
     */
    private RadarAvroConstants() {}
}
