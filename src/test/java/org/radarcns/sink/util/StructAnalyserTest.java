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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.util.StructAnalyser.JsonKey;

/**
 * {@link org.radarcns.sink.util.StructAnalyser} testcase.
 */
public class StructAnalyserTest {

    private List<org.apache.avro.Schema> input;
    private Map<String, JsonNode> output;

    /**
     * Initializer.
     */
    @Before
    public void setUp() throws IOException {
        this.input = new LinkedList<>();
        this.output = new HashMap<>();

        addMeasuramentKey();
    }

    @Test
    public void test() throws IOException {
        for (org.apache.avro.Schema schema : this.input) {
            assertEquals(this.output.get(schema.getFullName()),
                    StructAnalyser.analise(UtilityTest.avroToStruct(schema)));
        }
    }

    private void addMeasuramentKey() throws IOException {
        org.apache.avro.Schema schema = MeasurementKey.getClassSchema();
        this.input.add(schema);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(JsonKey.NAME.getParam(), schema.getFullName());
        objectNode.put(JsonKey.TYPE.getParam(), Type.STRUCT.getName().toUpperCase());
        objectNode.put(JsonKey.DOC.getParam(), schema.getDoc());
        objectNode.put(JsonKey.OPTIONAL.getParam(), Boolean.toString(false));

        ObjectNode userIdNode = mapper.createObjectNode();
        userIdNode.put(JsonKey.FIELD_NAME.getParam(), RadarAvroConstants.USER_ID);
        userIdNode.put(JsonKey.SCHEMA.getParam(), Schema.STRING_SCHEMA.toString());
        userIdNode.put(JsonKey.TYPE.getParam(), Schema.Type.STRING.getName().toUpperCase());
        userIdNode.put(JsonKey.DOC.getParam(), schema.getField(RadarAvroConstants.USER_ID).doc());
        userIdNode.put(JsonKey.OPTIONAL.getParam(), Boolean.toString(false));

        ArrayNode fields = objectNode.putArray(JsonKey.FIELDS.getParam());
        fields.add(userIdNode);

        ObjectNode sourceIdNode = mapper.createObjectNode();
        sourceIdNode.put(JsonKey.FIELD_NAME.getParam(), RadarAvroConstants.SOURCE_ID);
        sourceIdNode.put(JsonKey.SCHEMA.getParam(), Schema.STRING_SCHEMA.toString());
        sourceIdNode.put(JsonKey.TYPE.getParam(), Schema.Type.STRING.getName().toUpperCase());
        sourceIdNode.put(JsonKey.DOC.getParam(),
                schema.getField(RadarAvroConstants.SOURCE_ID).doc());
        sourceIdNode.put(JsonKey.OPTIONAL.getParam(), Boolean.toString(false));
        fields.add(sourceIdNode);

        JsonNode expected = mapper.readTree(objectNode.toString());

        this.output.put(MeasurementKey.getClassSchema().getFullName(), expected);
    }

}
