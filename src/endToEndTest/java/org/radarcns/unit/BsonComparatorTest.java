//package org.radarcns.unit;
//
//import static org.radarcns.sink.mongodb.util.MongoConstants.ID;
//import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
//import static org.radarcns.sink.mongodb.util.MongoConstants.TIMESTAMP;
//import static org.radarcns.sink.mongodb.util.MongoConstants.USER;
//
//import org.bson.BsonInt64;
//import org.bson.Document;
//import org.junit.Test;
//import org.radarcns.integration.BsonComparator;
//import org.radarcns.sink.mongodb.util.Converter;
//import org.radarcns.sink.mongodb.util.MongoConstants;
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
//public class BsonComparatorTest {
//
//    @Test
//    public void testComparator() throws Exception {
//
//        String USER_ID = "UserID_0";
//        String SOURCE_ID = "SourceID_0";
//
//        Document docTest = new Document(ID, USER_ID + "-" + SOURCE_ID)
//            .append(USER, USER_ID)
//            .append(SOURCE, SOURCE_ID)
//            .append(MongoConstants.RECORDS_CACHED, new BsonInt64(1000))
//            .append(MongoConstants.RECORDS_SENT, new BsonInt64(1000))
//            .append(MongoConstants.RECORDS_UNSENT, new BsonInt64(1000))
//            .append(TIMESTAMP, Converter.toDateTime(System.currentTimeMillis()));
//
//        BsonComparator.compareDocument(docTest, docTest, 0.0);
//    }
//
//}
