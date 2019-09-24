package org.apache.accumulo.spark.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.io.Text;

public class TopLevelGenericRecordBuilder extends SubGenericRecordBuilder {

	private Field field;

	public TopLevelGenericRecordBuilder(GenericRecordBuilder rootRecordBuilder, Field field) {
		super(rootRecordBuilder);

		this.field = field;
	}

	@Override
	public void set(Text columnQualifier, Object value) {
		rootRecordBuilder.set(this.field, value);
	}
}