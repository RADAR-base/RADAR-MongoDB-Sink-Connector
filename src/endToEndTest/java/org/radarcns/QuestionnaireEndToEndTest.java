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

import static org.radarcns.sink.util.MongoConstants.ANSWERS;
import static org.radarcns.sink.util.MongoConstants.END;
import static org.radarcns.sink.util.MongoConstants.ID;
import static org.radarcns.sink.util.MongoConstants.NAME;
import static org.radarcns.sink.util.MongoConstants.SOURCE;
import static org.radarcns.sink.util.MongoConstants.START;
import static org.radarcns.sink.util.MongoConstants.USER;
import static org.radarcns.sink.util.MongoConstants.VALUE;
import static org.radarcns.sink.util.RadarAvroConstants.SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.bson.Document;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Answer;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.questionnaire.QuestionnaireType;
import org.radarcns.sink.util.KeyGenerator;
import org.radarcns.sink.util.MongoConstants;
import org.radarcns.sink.util.MongoConstants.TypeLabel;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Sender;
import org.radarcns.util.SenderTestCase;

/**
 * MongoDB Sink connector e2e test. It streams a static generated {@link Questionnaire} message
 *      into the data pipeline and then queries MongoDb to check that data has been correctly
 *      stored.
 */
public class QuestionnaireEndToEndTest extends SenderTestCase {

    private static final QuestionnaireType TYPE = QuestionnaireType.PHQ8;
    private static final String VERSION = "v1.0";
    private static final String ANSWER = "I am fine";
    private static final double TIME = System.currentTimeMillis() / 1000d;

    @Override
    protected String getTopicName() {
        return "active_questionnaire_phq8";
    }

    @Override
    protected void send()
            throws IOException, IllegalAccessException, InstantiationException {
        List<Answer> answers = new ArrayList<>();
        answers.add(new Answer(ANSWER, TIME, TIME));

        MeasurementKey key = new MeasurementKey(USER_ID_MOCK, SOURCE_ID_MOCK);

        Questionnaire questionnaire = new Questionnaire(TYPE, VERSION, answers, TIME, TIME);

        AvroTopic<MeasurementKey, Questionnaire> topic =
                new AvroTopic<>(getTopicName(), MeasurementKey.getClassSchema(),
                Questionnaire.getClassSchema(), MeasurementKey.class, Questionnaire.class);

        Sender<MeasurementKey, Questionnaire> sender = new Sender<>(config, topic);

        sender.send(key, questionnaire);

        sender.close();
    }

    @Override
    protected Document getExpectedDocument() {
        Date timestamp = KeyGenerator.toDateTime(TIME);
        String keyTime = Long.toString(KeyGenerator.toDateTime(TIME).getTime());

        List<Document> answers = new LinkedList<>();
        answers.add(new Document(MongoConstants.TYPE, TypeLabel.STRING.getParam()).append(
                VALUE, ANSWER).append(START, timestamp).append(END, timestamp));

        return new Document(ID,
            USER_ID_MOCK.concat(SEPARATOR).concat(SOURCE_ID_MOCK).concat(SEPARATOR).concat(
                    keyTime).concat(SEPARATOR).concat(keyTime)).append(
            USER, USER_ID_MOCK).append(
            SOURCE, SOURCE_ID_MOCK).append(
            NAME, TYPE.name()).append(
            VERSION, VERSION).append(
            ANSWERS, answers).append(
            START, timestamp).append(
            END, timestamp);
    }
}
