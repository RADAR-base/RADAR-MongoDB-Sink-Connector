package org.radarcns.sink.mongodb;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.sink.mongodb.converter.DoubleArrayCollectorConverter;

/**
 * Created by nivethika on 2-3-17.
 */
public class DoubleArrayCollectorConverterTest {

    private DoubleArrayCollectorConverter converter;

    @Before
    public void setUp() {
        this.converter = new DoubleArrayCollectorConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(), 1, 0);
        assertEquals(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName(), values.toArray()[0]);
    }

////    @Test
//    public void convert() {
//
//        Schema keySchema = SchemaBuilder.struct()
//                .field(USER_ID, Schema.STRING_SCHEMA)
//                .field(SOURCE_ID, Schema.STRING_SCHEMA)
//                .field(RadarAvroConstants.START, Schema.INT64_SCHEMA)
//                .field(RadarAvroConstants.END, Schema.INT64_SCHEMA).build();
//
//        Struct keyStruct = new Struct(keySchema);
//        keyStruct.put(USER_ID, "user1");
//        keyStruct.put(SOURCE_ID, "source1");
//        keyStruct.put(RadarAvroConstants.START, (long)809923);
//        keyStruct.put(RadarAvroConstants.END, (long)3298989);
//
//
//        Schema quartileSchema = SchemaBuilder.struct().field("items",
//                  Schema.FLOAT64_SCHEMA).build();
//        Schema valueSchema = SchemaBuilder.struct().field("MINIMUM", Schema.FLOAT64_SCHEMA)
//                .field("MAXIMUM", Schema.FLOAT64_SCHEMA).field("SUM", Schema.FLOAT64_SCHEMA)
//                .field("COUNT", Schema.FLOAT64_SCHEMA).field("AVERAGE", Schema.FLOAT64_SCHEMA)
//                .field("QUARTILES", SchemaBuilder.array(quartileSchema))
//                .field("INTERQUARTILE_RANGE", Schema.FLOAT64_SCHEMA)
//                .build();
//        Double[] QUARTILES = new Double[]{232.3d, 23.d, 12.4d, 12.5d};
//        Struct valueStruct = new Struct(valueSchema);
//        valueStruct.put("MINIMUM", 100.0d);
//        valueStruct.put("MAXIMUM", 1001.0d);
//        valueStruct.put("SUM", 10.0d);
//        valueStruct.put("COUNT", 823.99d);
//        valueStruct.put("AVERAGE", 823.99d);
//        valueStruct.put("QUARTILES",QUARTILES);
//        valueStruct.put("INTERQUARTILE_RANGE", 290.6d);
//
//        SinkRecord record = new SinkRecord("mine", 0, keySchema,
//                keyStruct, valueSchema, valueStruct, 0);
//        Document document = this.converter.convert(record);
//
//        assertNotNull(document);
//        assertEquals(document.get("user"), "user1");
//        assertEquals(document.get("source"), "source1");
//        assertEquals(document.get("MINIMUM"), 100.0d);
//        assertEquals(document.get("MAXIMUM"), 1001);
////        assertEquals(document.get("recordsUnsent"), 10);
////        assertTrue(document.get("timestamp") instanceof BsonDateTime);
////        assertEquals(((BsonDateTime) document.get("timestamp")).getValue(), 823889);
//    }

}
