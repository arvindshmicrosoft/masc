package org.apache.accumulo.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroRowExpressionTest {

	@Test
	public void testExpressionColumn() throws IOException {
		SortedMap<Key, Value> map = new TreeMap<>();
		map.put(new Key("key1", "cf1", "cq1"), new Value("3"));
		map.put(new Key("key1", "cf2", ""), new Value("abc"));
		map.put(new Key("key2", "cf1", "cq1"), new Value("5"));
		map.put(new Key("key2", "cf2", ""), new Value("def"));

		SortedMapIterator parentIterator = new SortedMapIterator(map);
		AvroRowEncoderIterator iterator = new AvroRowEncoderIterator();

		Map<String, String> options = new HashMap<>();
		options.put(AvroRowEncoderIterator.SCHEMA,
				"[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"long\"},{\"cf\":\"cf2\",\"t\":\"STRING\"}]");

		// column.<column-name>.<type>
		options.put("column.cfExpr.double", "${cf1.cq1 + 5.2}");

		options.put("mleap.bundle", "JSON|protobuf");

		// TWITTER

		// fittedPipeline = ... // Spark ML pipeline
		// options.put("mleap.withColumn", true|false)
		// options.put("mleap.bundle", OurUtil.serialize(fittedPipeline))
		// .filter("prob > 0.8")

		// df.show()

		iterator.init(parentIterator, options, new DefaultIteratorEnvironment());
		iterator.seek(new Range(), AvroTestUtil.EMPTY_SET, false);

		SchemaMappingField[] schemaMappingFields = new SchemaMappingField[] {
				new SchemaMappingField("cf1", "cq1", "long", "v0"), new SchemaMappingField("cf2", null, "string", "v1"),
				new SchemaMappingField("cfExpr", null, "double", null) };

		Schema schema = AvroUtil.buildSchema(Arrays.asList(schemaMappingFields));

		// ############################## ROW 1
		assertTrue(iterator.hasTop());
		assertEquals("key1", iterator.getTopKey().getRow().toString());

		// validate value
		byte[] data = iterator.getTopValue().get();

		GenericRecord record = AvroTestUtil.deserialize(data, schema);
		GenericRecord cf1Record = (GenericRecord) record.get("cf1");

		assertEquals(3L, cf1Record.get("cq1"));
		assertEquals("abc", record.get("cf2").toString());
		assertEquals(8.2, record.get("cfExpr"));

		// End of data
		iterator.next();
		// assertFalse(iterator.hasTop());
	}
}