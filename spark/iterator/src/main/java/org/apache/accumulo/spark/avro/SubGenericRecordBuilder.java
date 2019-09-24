package org.apache.accumulo.spark.avro;

import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.io.Text;

// base class for populating the avro record
public abstract class SubGenericRecordBuilder {

	// root level builder
	protected GenericRecordBuilder rootRecordBuilder;

	public SubGenericRecordBuilder(GenericRecordBuilder rootRecordBuilder) {
		this.rootRecordBuilder = rootRecordBuilder;
	}

	public void set(Text columnQualifier, Object value) {
	}

	public void endRow() {
	}

	public void clear() {
	}
}