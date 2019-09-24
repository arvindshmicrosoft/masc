package org.apache.accumulo.spark.avro;

import org.apache.accumulo.spark.SchemaMappingField;
import org.apache.accumulo.spark.el.AvroContext;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

public class ExpressionColumn {
	private String column;

	private String type;

	private String expression;

	private ValueExpression columnExpression;

	private Field field;

	private AvroContext expressionContext;

	public ExpressionColumn(String column, String type, String expression) {
		this.column = column;
		this.type = type;
		this.expression = expression;
	}

	public String getColumn() {
		return column;
	}

	public SchemaMappingField getSchemaMappingField() {
		return new SchemaMappingField(this.column, null, this.type, null);
	}

	public void setField(Field field) {
		this.field = field;
	}

	private Class<?> getJavaType() {
		switch (type.toUpperCase()) {
		case "STRING":
			return String.class;

		case "LONG":
			return long.class;

		case "INTEGER":
			return int.class;

		case "DOUBLE":
			return double.class;

		case "FLOAT":
			return float.class;

		case "BOOLEAN":
			return boolean.class;

		default:
			throw new IllegalArgumentException("Unsupported type '" + type + "'");
		}
	}

	public void initialize(ExpressionFactory factory, AvroContext expressionContext) {
		this.expressionContext = expressionContext;
		this.columnExpression = factory.createValueExpression(expressionContext, expression, getJavaType());
	}

	public void populateField(GenericRecordBuilder rootRecordBuilder) {
		Object value = this.columnExpression.getValue(this.expressionContext);
		rootRecordBuilder.set(this.field, value);
	}
}