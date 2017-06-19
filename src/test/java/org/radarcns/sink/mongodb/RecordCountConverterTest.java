package org.radarcns.sink.mongodb;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
import static org.radarcns.sink.mongodb.util.MongoConstants.TIMESTAMP;
import static org.radarcns.sink.mongodb.util.MongoConstants.USER;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.SOURCE_ID;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.TIME_RECEIVED;
import static org.radarcns.sink.mongodb.util.RadarAvroConstants.USER_ID;

import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.application.ApplicationRecordCounts;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.mongodb.converter.RecordCountConverter;
import org.radarcns.sink.mongodb.util.MongoConstants;
import org.radarcns.sink.mongodb.util.RadarAvroConstants;

public class RecordCountConverterTest {

    private RecordCountConverter converter;

    @Before
    public void setUp() {
        this.converter = new RecordCountConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(MeasurementKey.class.getCanonicalName() + "-"
                + ApplicationRecordCounts.class.getCanonicalName(), values.toArray()[0]);
    }

    @Test
    public void convert() {

        Schema keySchema = SchemaBuilder
                .struct().field(USER_ID, Schema.STRING_SCHEMA)
                .field(SOURCE_ID, Schema.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema);
        keyStruct.put(USER_ID, "user1");
        keyStruct.put(SOURCE_ID, "source1");

        Schema valueSchema = SchemaBuilder.struct()
                .field(RadarAvroConstants.RECORDS_CACHED, Schema.INT32_SCHEMA)
                .field(RadarAvroConstants.RECORDS_SENT, Schema.INT32_SCHEMA)
                .field(RadarAvroConstants.RECORDS_UNSENT, Schema.INT32_SCHEMA)
                .field(TIME_RECEIVED, Schema.FLOAT64_SCHEMA).build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(RadarAvroConstants.RECORDS_CACHED, 100);
        valueStruct.put(RadarAvroConstants.RECORDS_SENT, 1001);
        valueStruct.put(RadarAvroConstants.RECORDS_UNSENT, 10);
        valueStruct.put(TIME_RECEIVED, 823.889d);

        SinkRecord record = new SinkRecord("mine", 0, keySchema,
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);
        assertEquals(document.get(USER), "user1");
        assertEquals(document.get(SOURCE), "source1");
        assertEquals(document.get(MongoConstants.RECORDS_CACHED), 100);
        assertEquals(document.get(MongoConstants.RECORDS_SENT), 1001);
        assertEquals(document.get(MongoConstants.RECORDS_UNSENT), 10);
        assertTrue(document.get(TIMESTAMP) instanceof BsonDateTime);
        assertEquals(((BsonDateTime) document.get(TIMESTAMP)).getValue(), 823889);
    }
}
