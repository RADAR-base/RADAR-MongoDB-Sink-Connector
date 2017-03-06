package org.radarcns.sink.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;

/**
 * Created by nivethika on 2-3-17.
 */
public class DoubleAggregatedRecordConverterTest {

    private DoubleAggregatedRecordConverter converter;

    @Before
    public void setUp() {
        this.converter = new DoubleAggregatedRecordConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName(), values.toArray()[0]);
    }

//    @Test
    public void convert() {

        Schema keySchema = SchemaBuilder
                .struct().field("userID", Schema.STRING_SCHEMA)
                .field("sourceID", Schema.STRING_SCHEMA).field("start", Schema.INT64_SCHEMA).field("end", Schema.INT64_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema);
        keyStruct.put("userID", "user1");
        keyStruct.put("sourceID", "source1");
        keyStruct.put("start", (long)809923);
        keyStruct.put("end", (long)3298989);


        Schema quartileSchema = SchemaBuilder.struct().field("items", Schema.FLOAT64_SCHEMA).build();
        Schema valueSchema = SchemaBuilder.struct().field("min", Schema.FLOAT64_SCHEMA)
                .field("max", Schema.FLOAT64_SCHEMA).field("sum", Schema.FLOAT64_SCHEMA)
                .field("count", Schema.FLOAT64_SCHEMA).field("avg", Schema.FLOAT64_SCHEMA)
                .field("quartile", SchemaBuilder.array(quartileSchema))
                .field("iqr", Schema.FLOAT64_SCHEMA)
                .build();
        Double[] quartile = new Double[]{232.3d, 23.d, 12.4d, 12.5d};
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put("min", 100.0d);
        valueStruct.put("max", 1001.0d);
        valueStruct.put("sum", 10.0d);
        valueStruct.put("count", 823.99d);
        valueStruct.put("avg", 823.99d);
        valueStruct.put("quartile",quartile);
        valueStruct.put("iqr", 290.6d);

        SinkRecord record = new SinkRecord("mine", 0, keySchema,
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);
        assertEquals(document.get("user"), "user1");
        assertEquals(document.get("source"), "source1");
        assertEquals(document.get("min"), 100.0d);
        assertEquals(document.get("max"), 1001);
//        assertEquals(document.get("recordsUnsent"), 10);
//        assertTrue(document.get("timestamp") instanceof BsonDateTime);
//        assertEquals(((BsonDateTime) document.get("timestamp")).getValue(), 823889);
    }

}
