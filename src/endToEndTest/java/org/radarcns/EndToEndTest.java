package org.radarcns;

/*
 * Copyright 2017 Kings College London and The Hyve
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

import static com.mongodb.client.model.Sorts.ascending;
import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.util.TestUtility.checkMongoDbConnection;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Answer;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.questionnaire.QuestionnaireType;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Sender;
import org.radarcns.util.TestCase;

/**
 * MongoDB Sink connector e2e test. It streams randomly generated data into the data pipeline and
 *      then queries MongoDb to check that data has been correctly stored. The check is done using
 *      precomputed expected data.
 */
public class EndToEndTest extends TestCase {

//    @Test
//    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
//    /**
//     * Test mock sensor specified in {@code resources/basic_mock_config.yml}.
//     */
//    public void endToEndTest() throws Exception {
//        produceInputFile();
//
//        Map<MockDataConfig, ExpectedValue> expectedValue = MockAggregator.getSimulations(
//                config.getData(), dataRoot);
//
//        final Map<MockDataConfig, List<Document>> expectedDocument = produceExpectedDocument(
//                expectedValue, new ExpectedDocumentFactory());
//
//        streamToKafka();
//
//        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
//        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));
//
//        checkMongoDbConnection();
//
//        fetchMongoDb(expectedDocument);
//    }

    @Test
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    /**
     * Test {@link Questionnaire} {@link org.apache.avro.specific.SpecificRecord}.
     */
    public void questionnaireTest() throws Exception {
        String topicName = "active_questionnaire_phq8";
        double time = System.currentTimeMillis() / 1000d;

        List<Answer> answers = new ArrayList<>();
        answers.add(new Answer("1", time, time));

        MeasurementKey key = new MeasurementKey(USER_ID_MOCK, SOURCE_ID_MOCK);
        Questionnaire questionnaire = new Questionnaire(time, time, QuestionnaireType.PHQ8,
            "v1.0", answers, time, time);

        AvroTopic<MeasurementKey, Questionnaire> topic =
            new AvroTopic<>(topicName, MeasurementKey.getClassSchema(),
                Questionnaire.getClassSchema(), MeasurementKey.class, Questionnaire.class);

        Sender<MeasurementKey, Questionnaire> sender = new Sender<>(config, topic);
        sender.send(key, questionnaire);
        sender.close();

        LOGGER.info("Waiting data ({} seconds) ... ", LATENCY);
        Thread.sleep(TimeUnit.SECONDS.toMillis(LATENCY));

        checkMongoDbConnection();

        FindIterable<Document> collection = hotstorage.getCollection(topicName).find().sort(
                ascending(ID));

        MongoCursor cursor = collection.iterator();
        if (cursor.hasNext()) {
            Document actualDoc = (Document) cursor.next();

            LOGGER.info("Actual: {}", actualDoc.toJson());
            Document expectedDoc = null;
//            assertDocument(expectedDoc, actualDoc, sensor.getMaximumDifference());
//            count++;
        }

    }
}
