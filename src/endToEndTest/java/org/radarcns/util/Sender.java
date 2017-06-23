package org.radarcns.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.data.SpecificRecordEncoder;
import org.radarcns.mock.config.BasicMockConfig;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.SchemaRetriever;
import org.radarcns.producer.rest.BatchedKafkaSender;
import org.radarcns.producer.rest.ConnectionState;
import org.radarcns.producer.rest.ManagedConnectionPool;
import org.radarcns.producer.rest.RestSender;
import org.radarcns.topic.AvroTopic;

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
public class Sender<K extends SpecificRecord, V extends SpecificRecord> {

    private static final int TIMEOUT = 60;

    private KafkaSender<K, V> kafkaSender;
    private KafkaTopicSender<K, V> kafkaTopicSender;
    private SchemaRetriever schemaRetriever;

    private int offset;

    public Sender(BasicMockConfig config, AvroTopic<K, V> topic)
            throws InstantiationException, IllegalAccessException, IOException {
        this.schemaRetriever = new SchemaRetriever(config.getSchemaRegistry(), TIMEOUT);
        this.kafkaSender = getSender(config);
        this.kafkaTopicSender = this.kafkaSender.sender(topic);

        this.offset = 0;
    }

    private KafkaSender<K, V> getSender(BasicMockConfig config) {
        ConnectionState sharedState = new ConnectionState(TIMEOUT, TimeUnit.SECONDS);

        RestSender.Builder<K, V> restBuilder = new RestSender.Builder<K, V>()
                .server(config.getRestProxy())
                .schemaRetriever(schemaRetriever)
                .useCompression(false)
                .encoders(new SpecificRecordEncoder(false),
                    new SpecificRecordEncoder(false))
                .connectionState(sharedState)
                .connectionTimeout(TIMEOUT, TimeUnit.SECONDS);

        RestSender<K, V> firstSender = restBuilder.connectionPool(
                new ManagedConnectionPool()).build();

        return new BatchedKafkaSender<>(firstSender, 1_000, 1000);
    }

    public void close() throws IOException {
        kafkaTopicSender.flush();
        kafkaTopicSender.close();
        kafkaSender.close();
        schemaRetriever.close();
    }

    public void send(K key, V value) throws IOException {
        kafkaTopicSender.send(Arrays.asList(new Record<>(offset, key, value)));
        offset++;
    }
}
