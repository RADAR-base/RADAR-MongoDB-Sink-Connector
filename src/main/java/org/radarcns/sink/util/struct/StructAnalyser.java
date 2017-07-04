package org.radarcns.sink.util.struct;

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

import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.ARRAY_ITEMS;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.DEFAULT_VALUE;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.DOC;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.FIELDS;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.FIELD_NAME;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.MAP_ITEMS;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.MAP_KEY;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.NAME;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.OPTIONAL;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.SCHEMA;
import static org.radarcns.sink.util.struct.StructAnalyser.JsonKey.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

/**
 * Static class for converting {@link Schema} into {@link JsonNode}.
 */
public final class StructAnalyser {

    /**
     * Enumerate all JSON keys used to create human readable representation of a Struct
     *      {@link Schema}.
     */
    public enum JsonKey {
        ARRAY_ITEMS("arrayItems"),
        DEFAULT_VALUE("defaultValue"),
        DOC("doc"),
        FIELDS("fields"),
        FIELD_NAME("fieldName"),
        MAP_KEY("mapKey"),
        MAP_ITEMS("mapItems"),
        NAME("name"),
        OPTIONAL("optional"),
        SCHEMA("schema"),
        TYPE("type");

        private final String param;

        JsonKey(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    private StructAnalyser() {
        //Static class
    }

    /**
     * Converts a {@link Schema} to a {@link String} stating the pretty JSON representation of
     *      the input schema.
     * @param schema Struct {@link Schema} to convert
     * @return a {@link String} stating the pretty JSON representation of the input schema
     * @throws IOException in case the input cannot be converted
     */
    public static String prettyAnalise(Schema schema) throws IOException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(
            analise(schema));
    }

    /**
     * Converts a {@link Schema} to a {@link JsonNode} useful for understanding how the struct
     *      contained in a {@link org.apache.kafka.connect.sink.SinkRecord} is schematised.
     *      the input schema.
     * @param schema Struct {@link Schema} to convert
     * @return {@link JsonNode} reporting all {@link Schema} meta-data and properties of each field
     *      contained in to it.
     * @throws IOException if something went wrong while converting
     */
    public static JsonNode analise(Schema schema) throws IOException {
        ObjectNode objectNode = analise(null, schema);

        JsonNode jsonNode = new ObjectMapper().readTree(objectNode.toString());

        return jsonNode;
    }

    /**
     * Analyser entry point.
     */
    private static ObjectNode analise(String fieldName, Schema schema) {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();

        if (fieldName != null) {
            objectNode = new ObjectMapper().createObjectNode();
            objectNode.put(FIELD_NAME.getParam(), fieldName);
            objectNode.put(SCHEMA.getParam(), schema.toString());
        }

        analiseField(objectNode, schema);

        switch (schema.type()) {
            case ARRAY:
                analiseFieldList(objectNode.putArray(ARRAY_ITEMS.getParam()),
                        schema.valueSchema().fields());
                break;
            case MAP:
                ObjectNode keyNode = new ObjectMapper().createObjectNode();
                analiseField(keyNode, schema.keySchema());
                objectNode.set(MAP_KEY.getParam(), keyNode);

                analiseFieldList(objectNode.putArray(MAP_ITEMS.getParam()),
                        schema.valueSchema().fields());
                break;
            case STRUCT:
                analiseFieldList(objectNode.putArray(FIELDS.getParam()), schema.fields());
                break;
            default: //nothing to do.
        }

        return objectNode;
    }

    /**
     * Adds general information contained into the Schema to the ObjectNode.
     */
    private static void analiseField(ObjectNode objectNode, Schema schema) {
        if (schema.name() != null) {
            objectNode.put(NAME.getParam(), schema.name());
        }

        if (schema.type() != null) {
            objectNode.put(TYPE.getParam(), schema.type().getName().toUpperCase());
        }

        if (schema.doc() != null) {
            objectNode.put(DOC.getParam(), schema.doc());
        }

        if (schema.defaultValue() != null) {
            objectNode.put(DEFAULT_VALUE.getParam(), schema.defaultValue().toString());
        }

        objectNode.put(OPTIONAL.getParam(), Boolean.toString(schema.isOptional()));
    }

    /**
     * Populates the ArrayNode with the information about each filed contained in the list.
     */
    private static void analiseFieldList(ArrayNode array, List<Field> list) {
        for (Field sibling : list) {
            array.add(analise(sibling.name(), sibling.schema()));
        }
    }
}
