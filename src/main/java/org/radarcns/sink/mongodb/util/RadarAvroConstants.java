package org.radarcns.sink.mongodb.util;

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
public class RadarAvroConstants {

    public static final String SEPARATOR = "-";

    public static final String USER_ID = "userID";
    public static final String SOURCE_ID = "sourceID";

    public static final String MIN = "MINIMUM";
    public static final String MAX = "MAX";
    public static final String SUM = "sum";
    public static final String COUNT = "COUNT";
    public static final String AVG = "AVERAGE";
    public static final String QUARTILE = "QUARTILES";
    public static final String IQR = "INTERQUARTILE_RANGE";

    public static final String START = "start";
    public static final String END = "end";

    public static final String RECORDS_CACHED = "recordsCached";
    public static final String RECORDS_SENT = "recordsSent";
    public static final String RECORDS_UNSENT = "recordsUnsent";

    public static final String SERVER_STATUS = "serverStatus";
    public static final String IP_ADDRESS = "ipAddress";

    public static final String UPTIME = "uptime";

    public static final String TIME_RECEIVED = "timeReceived";
}
