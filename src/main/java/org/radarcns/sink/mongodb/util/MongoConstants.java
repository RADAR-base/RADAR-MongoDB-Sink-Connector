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

public class MongoConstants {

    public static final String ID = "_id";
    public static final String USER = "user";
    public static final String SOURCE = "source";
    public static final String START = "start";
    public static final String END = "end";

    public static final String FIRST_QUARTILE = "25";
    public static final String SECOND_QUARTILE = "50";
    public static final String THIRD_QUARTILE = "75";

    public static final String RECORDS_CACHED = "recordsCached";
    public static final String RECORDS_SENT = "recordsSent";
    public static final String RECORDS_UNSENT = "recordsUnsent";

    public static final String SERVER_STATUS = "serverStatus";
    public static final String CLIENT_IP = "clientIP";

    public static final String APPLICATION_UPTIME = "applicationUptime";

    public static final String TIMESTAMP = "timestamp";

    /**
     * Enumerate all available statistical values. The string value represents the Document field
     *      that has to be used to compute the result.
     */
    public enum Stat {
        AVERAGE("AVERAGE"),
        COUNT("COUNT"),
        INTERQUARTILE_RANGE("INTERQUARTILE_RANGE"),
        MAXIMUM("MAX"),
        MEDIAN("QUARTILES"),
        MINIMUM("MINIMUM"),
        QUARTILES("QUARTILES"),
        SUM("sum");

        private final String param;

        Stat(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

}
