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

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Converter from AVRO {@link org.apache.avro.Schema} to Struct
 *      {@link org.apache.kafka.connect.data.Schema}.
 */
@SuppressWarnings("PMD.GodClass")
public final class AvroToStruct {

    public static final String ARRAY_LABEL = "array";
    public static final String MAP_LABEL = "map";
    public static final String UNION_LABEL = "io.confluent.connect.avro.Union";

    private AvroToStruct() {
        //Final class
    }

    /**
     * Convert the input AVRO {@link org.apache.avro.Schema} into an equivalent
     *      Struct {@link Schema}.
     * @param avroSchema AVRO {@link org.apache.avro.Schema} to converted
     * @return a Struct {@link Schema} equivalent to the input AVRO {@link org.apache.avro.Schema}
     */
    public static Schema convertSchema(org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case RECORD: return getRecordSchema(avroSchema, false);
            case ENUM: return new ConnectSchema(Schema.Type.STRING, false, null,
                avroSchema.getFullName(), null, avroSchema.getDoc(), null,
                null, null, null);
            default: throw new ConverterRuntimeException(avroSchema.getFullName()
                + " cannot be converted");
        }
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Field} into the equivalent Struct
     *      {@link Schema} representation.
     * @param field {@link org.apache.avro.Schema.Field} to be converted in Struct {@link Schema}
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    @SuppressWarnings("PMD.StdCyclomaticComplexity")
    private static Schema convertSchema(Field field) {
        org.apache.avro.Schema avroSchema = field.schema();

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
            case RECORD: return convertSchema(avroSchema);
            case STRING: return Schema.STRING_SCHEMA;
            case UNION: return getUnionSchema(field, false);
            default: throw new ConverterRuntimeException(field.schema().getType()
                + " cannot be converted");
        }
    }

    /**
     * Convert the input {@link org.apache.avro.Schema} into the equivalent Struct
     *      {@link Schema} representation setting it as optional if needed. A schema is optional if
     *      listed by an AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema {@link org.apache.avro.Schema} to be converted in optional Struct
     *      {@link Schema}
     * @return optional Struct {@link Schema} equivalent to the input
     *      {@link org.apache.avro.Schema.Field}
     */
    @SuppressWarnings("PMD.StdCyclomaticComplexity")
    private static Schema optionalSchema(org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case ARRAY: return getArraySchema(avroSchema, true);
            case BYTES: return Schema.OPTIONAL_BYTES_SCHEMA;
            case BOOLEAN: return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case DOUBLE: return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case ENUM: return getEnumSchema(avroSchema, true);
            case FLOAT: return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case INT: return Schema.OPTIONAL_INT32_SCHEMA;
            case LONG: return Schema.OPTIONAL_INT64_SCHEMA;
            case MAP: return getMapSchema(avroSchema, true);
            case RECORD: return getRecordSchema(avroSchema, true);
            case STRING: return Schema.OPTIONAL_STRING_SCHEMA;
            default: throw new ConverterRuntimeException(avroSchema.getType() + " cannot be "
                + "converted in OPTIONAL type schema");
        }
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#ENUM}
     *      {@link org.apache.avro.Schema.Field} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param field {@link org.apache.avro.Schema.Field} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    private static Schema getEnumSchema(Field field, boolean optional) {
        return getEnumSchema(field.schema(), field.defaultVal(), field.doc(), optional);
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#ENUM}
     *      {@link org.apache.avro.Schema} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema {@link org.apache.avro.Schema} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema}
     */
    private static Schema getEnumSchema(org.apache.avro.Schema avroSchema, boolean optional) {
        return getEnumSchema(avroSchema, null, avroSchema.getDoc(), optional);
    }

    /**
     * Convert an {@link org.apache.avro.Schema.Type#ENUM} {@link org.apache.avro.Schema} into
     *      the equivalent Struct {@link Schema} representation using all available parameters.
     *      A schema is optional if listed by an AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema AVRO {@link org.apache.avro.Schema} to be converted
     * @param defaultValue {@link org.apache.avro.Schema.Field} may have a default value, this must
     *      be reflected by the Struct {@link Schema} as well
     * @param documentation coming from the related {@link org.apache.avro.Schema.Field}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema} and
     *      meta-data
     */
    private static Schema getEnumSchema(org.apache.avro.Schema avroSchema, Object defaultValue,
            String documentation, boolean optional) {
        return new ConnectSchema(Schema.Type.STRING, optional, defaultValue,
            avroSchema.getFullName(),null, documentation, null,null,
            null, null);
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#ARRAY}
     *      {@link org.apache.avro.Schema.Field} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param field {@link org.apache.avro.Schema.Field} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    private static Schema getArraySchema(Field field, boolean optional) {
        return getArraySchema(field.name(), field.schema().getElementType(), field.defaultVal(),
                field.doc(), optional);
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#ARRAY}
     *      {@link org.apache.avro.Schema} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema {@link org.apache.avro.Schema} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    private static Schema getArraySchema(org.apache.avro.Schema avroSchema, boolean optional) {
        return getArraySchema(ARRAY_LABEL, avroSchema.getElementType(), null,
            avroSchema.getDoc(), optional);
    }

    /**
     * Convert an {@link org.apache.avro.Schema.Type#ARRAY} {@link org.apache.avro.Schema} into
     *      the equivalent Struct {@link Schema} representation using all available parameters.
     *      A schema is optional if listed by an AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param name schema name
     * @param schemaValue AVRO {@link org.apache.avro.Schema} representing the array item
     * @param defaultValue {@link org.apache.avro.Schema.Field} may have a default value, this must
     *      be reflected by the Struct {@link Schema} as well
     * @param documentation coming from the related {@link org.apache.avro.Schema.Field}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input meta-data
     */
    private static Schema getArraySchema(String name, org.apache.avro.Schema schemaValue,
            Object defaultValue, String documentation, boolean optional) {
        Schema valueSchema = convertSchema(schemaValue);

        SchemaBuilder schemaBuilder = SchemaBuilder.array(valueSchema).name(name);

        schemaBuilder = setDocDefaultOptional(schemaBuilder, defaultValue, documentation, optional);

        return schemaBuilder.build();
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#MAP}
     *      {@link org.apache.avro.Schema.Field} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param field {@link org.apache.avro.Schema.Field} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    private static Schema getMapSchema(Field field, boolean optional) {
        return getMapSchema(field.name(), field.schema().getValueType(), field.defaultVal(),
            field.doc(), optional);
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#MAP}
     *      {@link org.apache.avro.Schema} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema {@link org.apache.avro.Schema} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema}
     */
    private static Schema getMapSchema(org.apache.avro.Schema avroSchema, boolean optional) {
        return getMapSchema(MAP_LABEL, avroSchema.getValueType(), null,
                avroSchema.getDoc(), optional);
    }

    /**
     * Convert an {@link org.apache.avro.Schema.Type#MAP} {@link org.apache.avro.Schema} into
     *      the equivalent Struct {@link Schema} representation using all available parameters.
     *      A schema is optional if listed by an AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param name schema name
     * @param schemaValue AVRO {@link org.apache.avro.Schema} representing the array item
     * @param defaultValue {@link org.apache.avro.Schema.Field} may have a default value, this must
     *      be reflected by the Struct {@link Schema} as well
     * @param documentation coming from the related {@link org.apache.avro.Schema.Field}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema} and
     *      meta-data
     */
    private static Schema getMapSchema(String name, org.apache.avro.Schema schemaValue,
            Object defaultValue, String documentation, boolean optional) {
        Schema keySchema = Schema.STRING_SCHEMA;
        Schema valueSchema = convertSchema(schemaValue);

        SchemaBuilder schemaBuilder = SchemaBuilder.map(keySchema, valueSchema).name(name);

        schemaBuilder = setDocDefaultOptional(schemaBuilder, defaultValue, documentation, optional);

        return schemaBuilder.build();
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#RECORD}
     *      {@link org.apache.avro.Schema} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param avroSchema {@link org.apache.avro.Schema} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema}
     */
    private static Schema getRecordSchema(org.apache.avro.Schema avroSchema, boolean optional) {
        List<org.apache.kafka.connect.data.Field> fieldList = new ArrayList<>();

        for (Field field : avroSchema.getFields()) {
            fieldList.add(new org.apache.kafka.connect.data.Field(field.name(),
                    fieldList.size(), convertSchema(field)));
        }

        return new ConnectSchema(Schema.Type.STRUCT, optional, null,
            avroSchema.getFullName(), null, avroSchema.getDoc(), null,
            fieldList, null, null);
    }

    /**
     * Convert the input {@link org.apache.avro.Schema.Type#UNION}
     *      {@link org.apache.avro.Schema.Field} into the equivalent Struct {@link Schema}
     *      representation setting it as optional if needed. A schema is optional if listed by an
     *      AVRO {@link org.apache.avro.Schema.Type#UNION}.
     * @param field {@link org.apache.avro.Schema.Field} to be converted in Struct {@link Schema}
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return Struct {@link Schema} equivalent to the input {@link org.apache.avro.Schema.Field}
     */
    private static Schema getUnionSchema(Field field, boolean optional) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(UNION_LABEL).doc(field.doc());

        for (org.apache.avro.Schema avroSchema : field.schema().getTypes()) {
            schemaBuilder.field(avroSchema.getName(), optionalSchema(avroSchema));
        }

        schemaBuilder = setDocDefaultOptional(schemaBuilder, field.defaultVal(), field.doc(),
                optional);

        return schemaBuilder.build();
    }

    /**
     * Set default value, documentation and the optional flag to the given {@link SchemaBuilder}.
     *
     * @param schemaBuilder {@link SchemaBuilder} to update
     * @param defaultValue {@link Object} representing default value for a
     *      {@link org.apache.avro.Schema.Field}
     * @param documentation documentation about the schema
     * @param optional {@code true} if optional, {@code false} otherwise
     * @return {@link SchemaBuilder} equals to the input one and enhanced with the input metadata
     *
     * @see Field#defaultVal()
     */
    private static SchemaBuilder setDocDefaultOptional(SchemaBuilder schemaBuilder,
            Object defaultValue, String documentation, boolean optional) {
        SchemaBuilder local = schemaBuilder;
        if (documentation != null) {
            local = local.doc(documentation);
        }

        if (defaultValue != null) {
            local = local.defaultValue(defaultValue);
        }

        if (optional) {
            local = local.optional();
        }

        return local;
    }

}
