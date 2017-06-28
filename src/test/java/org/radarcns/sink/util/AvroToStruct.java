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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.key.MeasurementKey;
import org.radarcns.questionnaire.Questionnaire;
import org.radarcns.sink.util.exception.ConverterRuntimeException;

/**
 * Converts an AVRO {@link org.apache.avro.Schema} in to the corresponding
 *      {@link org.apache.kafka.connect.data.Schema}.
 */
public class AvroToStruct {

    private List<org.apache.avro.Schema> input;

    /**
     * Initializer.
     */
    @Before
    public void setUp() {
        this.input = new LinkedList<>();
        this.input.add(MeasurementKey.getClassSchema());
        this.input.add(Questionnaire.getClassSchema());
    }

    @Test
    public void test() throws IOException {
        for (org.apache.avro.Schema schema : this.input) {
            Schema dataSchema = convertSchema(schema);
            analiseStruct(dataSchema);
        }
    }

    public static Schema convertSchema(org.apache.avro.Schema avroSchema) {
        if (avroSchema.getType().equals(Type.RECORD)) {
            List<org.apache.kafka.connect.data.Field> fieldList = new ArrayList<>();
            //System.out.println("Name: " + avroSchema.getFullName());
            //System.out.println("Type: " + avroSchema.getType());

            for (Field field : avroSchema.getFields()) {
                fieldList.add(new org.apache.kafka.connect.data.Field(field.name(),
                    fieldList.size(), convertSchema(field)));
            }

            return new ConnectSchema(Schema.Type.STRUCT, false, null,
                    avroSchema.getFullName(), null, avroSchema.getDoc(), null,
                    fieldList, null, null);
        } else if (avroSchema.getType().equals(Type.ENUM)) {
            //System.out.println("Name: " + avroSchema.getFullName());
            //System.out.println("Type: " + avroSchema.getType());

            return new ConnectSchema(Schema.Type.STRING, false, null,
                    avroSchema.getFullName(), null, avroSchema.getDoc(), null,
                    null, null, null);
        }

        throw new ConverterRuntimeException(avroSchema.getFullName() + " cannot be converted");
    }

    private static Schema convertSchema(Field field) {
        org.apache.avro.Schema avroSchema = field.schema();

        //System.out.println("Field name: " + field.name());
        //System.out.println("Field schema: " + field.schema().toString(true));
        //System.out.println("-----------");

        switch (avroSchema.getType()) {
            case ARRAY: return getArraySchema(field, false);
            case BYTES: return Schema.BYTES_SCHEMA;
            case BOOLEAN: return Schema.BOOLEAN_SCHEMA;
            case DOUBLE: return Schema.FLOAT64_SCHEMA;
            case ENUM: return getEnumSchema(field, false);
            case FLOAT: return Schema.FLOAT32_SCHEMA;
            case INT: return Schema.INT32_SCHEMA;
            case LONG: return Schema.INT64_SCHEMA;
            case MAP: return getMapSchema(field, false);
            case STRING: return Schema.STRING_SCHEMA;
            case UNION: return getUnionSchema(field, false);
            default: throw new ConverterRuntimeException(field.schema().getType()
                    + " cannot be converted");
        }

    }

    private static Schema getEnumSchema(Field field, boolean optional) {
        return new ConnectSchema(Schema.Type.STRING, optional, field.defaultVal(),
                field.schema().getFullName(),null, field.doc(), null,
                null, null, null);
    }

    private static Schema getArraySchema(Field field, boolean optional) {
        Schema valueSchema = convertSchema(field.schema().getElementType());

        SchemaBuilder schemaBuilder = SchemaBuilder.array(valueSchema)
                .name(field.name())
                .doc(field.doc());

        if (optional) {
            schemaBuilder = schemaBuilder.optional();
        }

        if (field.defaultVal() != null) {
            schemaBuilder = schemaBuilder.defaultValue(field.defaultVal());
        }

        return schemaBuilder.build();
    }

    private static Schema getMapSchema(Field field, boolean optional) {
        Schema keySchema = Schema.STRING_SCHEMA;
        Schema valueSchema = convertSchema(field.schema().getValueType());

        SchemaBuilder schemaBuilder = SchemaBuilder.map(keySchema, valueSchema)
            .name(field.name())
            .doc(field.doc());

        if (optional) {
            schemaBuilder = schemaBuilder.optional();
        }

        if (field.defaultVal() != null) {
            schemaBuilder = schemaBuilder.defaultValue(field.defaultVal());
        }

        return schemaBuilder.build();
    }

    private static Schema getUnionSchema(Field field, boolean optional) {
        //System.out.println(field.name());
        //System.out.println(field.schema().toString(true));

        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name("io.confluent.connect.avro.Union")
                .doc(field.doc());

        for (org.apache.avro.Schema avroSchema : field.schema().getTypes()) {
            //System.out.println("getName " + avroSchema.getName());
            //System.out.println("schema " + avroSchema.toString(true));
            schemaBuilder.field(avroSchema.getName(), optionalSchema(avroSchema));
        }

        if (optional) {
            schemaBuilder = schemaBuilder.optional();
        }

        if (field.defaultVal() != null) {
            schemaBuilder = schemaBuilder.defaultValue(field.defaultVal());
        }

        return schemaBuilder.build();
    }

    private static Schema optionalSchema(org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case BYTES: return Schema.OPTIONAL_BYTES_SCHEMA;
            case BOOLEAN: return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case DOUBLE: return Schema.OPTIONAL_FLOAT64_SCHEMA;
            //case ENUM: return getEnumSchema(Schema.Type.STRING, field, false);
            case FLOAT: return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case INT: return Schema.OPTIONAL_INT32_SCHEMA;
            case LONG: return Schema.OPTIONAL_INT64_SCHEMA;
            //case MAP: throw new ConverterRuntimeException(field.schema().getType()
            // + " not supported yet");
            //case RECORD: throw new ConverterRuntimeException(field.schema().getType()
            // + " not supported yet");
            case STRING: return Schema.OPTIONAL_STRING_SCHEMA;
            default: throw new ConverterRuntimeException(avroSchema.getType() + " cannot be "
                    + "converted in OPTIONAL type schema");
        }
    }

    //private static void printAvroSchema(org.apache.avro.Schema avroSchema)
            //throws IOException {
        //System.out.println(avroSchema.toString(true));
    //}

    private JsonNode analiseStruct(Schema schema) throws IOException {
        ObjectNode objectNode = analiseStruct(null, schema);

        JsonNode jsonNode = new ObjectMapper().readTree(objectNode.toString());

        //System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));

        return jsonNode;
    }

    private ObjectNode analiseStruct(String fieldName, Schema schema) {

        ObjectNode objectNode = new ObjectMapper().createObjectNode();

        if (fieldName != null) {
            objectNode = new ObjectMapper().createObjectNode();
            objectNode.put("fieldName", fieldName);
            objectNode.put("schema", schema.toString());
        }

        if (schema.name() != null) {
            objectNode.put("name", schema.name());
        }

        if (schema.type() != null) {
            objectNode.put("type", schema.type().getName().toUpperCase());
        }

        if (schema.doc() != null) {
            objectNode.put("doc", schema.doc());
        }

        if (schema.defaultValue() != null) {
            objectNode.put("defaultValue", schema.defaultValue().toString());
        }

        objectNode.put("optional", Boolean.toString(schema.isOptional()));

        if (schema.type().equals(Schema.Type.ARRAY)) {
            ArrayNode arrayItems = objectNode.putArray("arrayItems");
            for (org.apache.kafka.connect.data.Field sibling : schema.valueSchema().fields()) {
                arrayItems.add(analiseStruct(sibling.name(), sibling.schema()));
            }
        } else if (schema.type().equals(Schema.Type.MAP)) {
            ObjectNode keyNode = new ObjectMapper().createObjectNode();

            if (schema.keySchema().name() != null) {
                keyNode.put("name", schema.keySchema().name());
            }

            if (schema.keySchema().type() != null) {
                keyNode.put("type", schema.keySchema().type().getName());
            }

            if (schema.keySchema().doc() != null) {
                keyNode.put("doc", schema.keySchema().doc());
            }

            if (schema.keySchema().defaultValue() != null) {
                keyNode.put("default", schema.keySchema().defaultValue().toString());
            }

            objectNode.set("mapKey", keyNode);

            ArrayNode mapItems = objectNode.putArray("mapItems");
            for (org.apache.kafka.connect.data.Field sibling : schema.valueSchema().fields()) {
                mapItems.add(analiseStruct(sibling.name(), sibling.schema()));
            }
        } else if (schema.type().equals(Schema.Type.STRUCT)) {
            ArrayNode fields = objectNode.putArray("fields");
            for (org.apache.kafka.connect.data.Field sibling : schema.fields()) {
                fields.add(analiseStruct(sibling.name(), sibling.schema()));
            }
        }

        return objectNode;
    }
}
