package org.radarcns.sink.mongodb.util;

import org.apache.kafka.connect.data.Struct;

public final class Utility {

    private Utility() {
               // utility class
    }

    public static String intervalKeyToMongoKey(Struct key) {
        return key.get("userID")
                + "-" + key.get("sourceID")
                + "-" + key.get("start")
                + "-" + key.get("end");

    }

}