package org.radarcns.sink.mongodb;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

public class CountsStatusRecordConverterTest {

    private CountsStatusRecordConverter converter;

    @Before
    public void setUp() {
        this.converter = new CountsStatusRecordConverter();
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
                .struct().field("userId", Schema.STRING_SCHEMA)
                .field("sourceId", Schema.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema);
        keyStruct.put("userId", "user1");
        keyStruct.put("sourceId", "source1");

        Schema valueSchema = SchemaBuilder.struct().field("recordsCached", Schema.INT32_SCHEMA)
                .field("recordsSent", Schema.INT32_SCHEMA)
                .field("recordsUnsent", Schema.INT32_SCHEMA)
                .field("timeReceived", Schema.FLOAT64_SCHEMA).build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put("recordsCached", 100);
        valueStruct.put("recordsSent", 1001);
        valueStruct.put("recordsUnsent", 10);
        valueStruct.put("timeReceived", 823.889d);

        SinkRecord record = new SinkRecord("mine", 0, keySchema,
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);
        assertEquals(document.get("user"), "user1");
        assertEquals(document.get("source"), "source1");
        assertEquals(document.get("recordsCached"), 100);
        assertEquals(document.get("recordsSent"), 1001);
        assertEquals(document.get("recordsUnsent"), 10);
        assertTrue(document.get("timestamp") instanceof BsonDateTime);
        assertEquals(((BsonDateTime) document.get("timestamp")).getValue(), 823889);
    }
}
