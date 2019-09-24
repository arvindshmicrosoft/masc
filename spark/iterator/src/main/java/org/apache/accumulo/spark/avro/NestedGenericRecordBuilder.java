package org.apache.accumulo.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.io.Text;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.Function;

public class NestedGenericRecordBuilder extends SubGenericRecordBuilder {
	// used for fast clear
	private Field[] nestedFields;
	// wrapped builder
	private GenericRecordBuilder recordBuilder;
	// used for fast value setting
	private Map<Text, Field> fieldLookup;
	// co-relate back to parent record
	private Field parentField;

	public NestedGenericRecordBuilder(GenericRecordBuilder rootRecordBuilder, Schema schema, Field parentField) {
		super(rootRecordBuilder);

		this.parentField = parentField;

		nestedFields = schema.getFields().toArray(new Field[0]);

		recordBuilder = new GenericRecordBuilder(schema);

		// make sure we made from Text (not string) to field
		// a) this avoids memory allocation for the string object
		// b) this allows use to get the field index in the avro record directly (no
		// field index lookup required)
		fieldLookup = Stream.of(nestedFields).collect(Collectors.toMap(f -> new Text(f.name()), Function.identity()));
	}

	public Field getParentField() {
		return parentField;
	}

	@Override
	public void set(Text columnQualifer, Object value) {
		// from hadoop text to field pos
		recordBuilder.set(fieldLookup.get(columnQualifer), value);
	}

	@Override
	public void clear() {
		for (Field field : nestedFields)
			recordBuilder.clear(field);
	}

	@Override
	public void endRow() {
		rootRecordBuilder.set(parentField, recordBuilder.build());
	}
}
