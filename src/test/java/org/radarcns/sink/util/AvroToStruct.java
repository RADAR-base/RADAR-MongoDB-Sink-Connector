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
import org.radarcns.avro.restapi.app.ServerStatus;
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
        this.input.add(ServerStatus.getClassSchema());
    }

    @Test
    public void test() throws IOException {
        for (org.apache.avro.Schema schema : this.input) {
            printAvroSchema(schema);

            System.out.println("-----------");

            Schema dataSchema = convertSchema(schema);
            analiseStruct(dataSchema, schema.getFullName());
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

    private void analiseStruct(Schema schema, String element) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[" + element.toUpperCase() + "]" + '\n');
        stringBuilder.append("Schema: " + schema.toString() + '\n');

        analiseStruct(null, schema, stringBuilder, 1);

        //System.out.println(stringBuilder.toString());
    }

    private void analiseStruct(String fieldName, Schema schema, StringBuilder stringBuilder,
            int space) {
        String blank = "";
        for (int i = 0; i < space; i++) {
            blank += " ";
        }

        if (fieldName != null)
            stringBuilder.append(blank + "Name: " + fieldName + "\t Schema: "
                    + schema.toString() + '\n');

        if (schema.name() != null) stringBuilder.append(blank + "[SCHEMA] name: "
                + schema.name() + '\n');
        if (schema.type() != null) stringBuilder.append(blank + "[SCHEMA] type: "
                + schema.type() + '\n');
        if (schema.doc() != null) stringBuilder.append(blank + "[SCHEMA] doc: "
                + schema.doc() + '\n');
        if (schema.defaultValue() != null) stringBuilder.append(blank + "[SCHEMA] default: "
                + schema.defaultValue().toString() + '\n');
        stringBuilder.append(blank + "[SCHEMA] optional: "
                + Boolean.toString(schema.isOptional()) + '\n');

        if (schema.type().equals(Schema.Type.ARRAY)) {
            stringBuilder.append(blank + "[SCHEMA] VALUE" + '\n');
            for (org.apache.kafka.connect.data.Field sibling : schema.valueSchema().fields()) {
                analiseStruct(sibling.name(), sibling.schema(), stringBuilder, space + 1);
            }
        } else if (schema.type().equals(Schema.Type.MAP)) {
            stringBuilder.append(blank + "[SCHEMA] KEY" + '\n');
            if (schema.keySchema().name() != null)
                    stringBuilder.append(blank + "[SCHEMA] name: "
                            + schema.keySchema().name() + '\n');
            if (schema.keySchema().type() != null)
                    stringBuilder.append(blank + "[SCHEMA] type: "
                            + schema.keySchema().type() + '\n');
            if (schema.keySchema().doc() != null)
                    stringBuilder.append(blank + "[SCHEMA] doc: "
                            + schema.keySchema().doc() + '\n');
            if (schema.keySchema().defaultValue() != null)
                    stringBuilder.append(blank + "[SCHEMA] default: "
                            + schema.keySchema().defaultValue().toString() + '\n');

            stringBuilder.append(blank + "[SCHEMA] VALUE" + '\n');
            for (org.apache.kafka.connect.data.Field sibling : schema.valueSchema().fields()) {
                analiseStruct(sibling.name(), sibling.schema(), stringBuilder, space + 1);
            }
        } else if (schema.type().equals(Schema.Type.STRUCT)) {
            for (org.apache.kafka.connect.data.Field sibling : schema.fields()) {
                analiseStruct(sibling.name(), sibling.schema(), stringBuilder, space + 1);
            }
        }
    }
}
