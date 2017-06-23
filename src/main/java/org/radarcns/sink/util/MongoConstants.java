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

/**
 * Set of constants used to create {@link org.bson.Document}.
 */
public final class MongoConstants {

    public static final String ID = "_id";
    public static final String USER = "user";
    public static final String SOURCE = "source";
    public static final String START = "start";
    public static final String END = "end";

    public static final String FIRST_QUARTILE = "25";
    public static final String SECOND_QUARTILE = "50";
    public static final String THIRD_QUARTILE = "75";

    public static final String RECORDS_CACHED = RadarAvroConstants.RECORDS_CACHED;
    public static final String RECORDS_SENT = RadarAvroConstants.RECORDS_SENT;
    public static final String RECORDS_UNSENT = RadarAvroConstants.RECORDS_UNSENT;

    public static final String SERVER_STATUS = RadarAvroConstants.SERVER_STATUS;
    public static final String CLIENT_IP = "clientIP";

    public static final String APPLICATION_UPTIME = "applicationUptime";

    public static final String TIMESTAMP = "timestamp";

    public static final String X_LABEL = "x";
    public static final String Y_LABEL = "y";
    public static final String Z_LABEL = "z";

    /**
     * Private constructor to prevent instantiation.
     */
    private MongoConstants() {}

    /**
     * Enumerate all available statistical values. The string value represents the name of Bson
     *      Document field.
     */
    public enum Stat {
        AVERAGE("avg"),
        COUNT("count"),
        INTERQUARTILE_RANGE("iqr"),
        MAXIMUM("max"),
        MEDIAN("quartile"),
        MINIMUM("min"),
        QUARTILES("quartile"),
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
