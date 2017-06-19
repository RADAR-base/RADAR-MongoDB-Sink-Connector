//package org.radarcns.integration;
//
//import static org.junit.Assert.assertEquals;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//import org.bson.Document;
//
///*
// * Copyright 2017 King's College London and The Hyve
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//public class BsonComparator {
//
//    private enum BsonJavaComparator {
//        DEFAULT {
//            public void compare(Object expected, Object actual, Double delta) {
//                assertEquals(expected, actual);
//            }
//        },
//        BSONINT64 {
//            public void compare(Object expected, Object actual, Double delta) {
//                assertEquals((Long)expected, (Long)actual, delta);
//            }
//        };
//
//        abstract void compare(Object expected, Object actual, Double delta);
//
//        public static final List<String> TYPE_NAMES = Arrays.stream(BsonJavaComparator.values())
//            .map(Object::toString)
//            .collect(Collectors.toList());
//
//        public static BsonJavaComparator getType(Object o) {
//            String simpleName = o.getClass().getSimpleName().toUpperCase();
//
//            if (TYPE_NAMES.contains(simpleName)) {
//                return BsonJavaComparator.valueOf(simpleName);
//            } else {
//                return BsonJavaComparator.DEFAULT;
//            }
//
//            throw new DataException("Data type " + o.getClass() + " not recognized");
//        }
//    }
//
//    public static void compareDocument(Document expected, Document actual, Double delta) {
//        assertEquals(expected.keySet(), actual.keySet());
//
//        for (String key : expected.keySet()) {
//            compareValue(expected.get(key), actual.get(key), delta);
//
//
//        }
//
//    }
//
//    private static void compareValue(Object expected, Object actual, Double delta) {
//        assertEquals(expected.getClass(), actual.getClass());
//
//        System.out.println(expected.getClass().getSimpleName().toUpperCase());
//
//    }
//}
