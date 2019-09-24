package org.apache.accumulo.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.spark.avro.ExpressionColumn;
import org.apache.accumulo.spark.avro.NestedGenericRecordBuilder;
import org.apache.accumulo.spark.avro.SubGenericRecordBuilder;
import org.apache.accumulo.spark.avro.TopLevelGenericRecordBuilder;
import org.apache.accumulo.spark.el.AvroContext;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Text;

public class AvroRowEncoderIterator extends BaseMappingIterator {
  public static final String FILTER = "filter";
  public static final String COLUMN_PREFIX = "column.";
  public static final String MLEAP_BUNDLE = "column.";

  // root schema holding fields for each column family
  private Schema schema;

  // avro writer infra
  private ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();
  private DatumWriter<GenericRecord> writer;
  private BinaryEncoder encoder;

  // record builder for the root
  private GenericRecordBuilder rootRecordBuilder;

  // fast lookup by using Text as key type
  private Map<Text, SubGenericRecordBuilder> columnFamilyRecordBuilder = new HashMap<>();

  private List<ExpressionColumn> expressionColumns;

  // allocate once to re-use memory
  private Text columnFamilyText = new Text();
  private Text columnQualifierText = new Text();
  private AvroContext expressionContext;
  private ValueExpression filterExpression;

  @Override
  protected void startRow(Text rowKey) throws IOException {
    binaryBuffer.reset();

    // clear all fields
    for (SubGenericRecordBuilder builder : columnFamilyRecordBuilder.values())
      builder.clear();
  }

  @Override
  protected void processCell(Key key, Value value, Object decodedValue) throws IOException {
    // passing columnFamilyText to re-use memory
    SubGenericRecordBuilder builder = columnFamilyRecordBuilder.get(key.getColumnFamily(columnFamilyText));

    // passing columnQualifierText to re-use memory
    builder.set(key.getColumnQualifier(columnQualifierText), decodedValue);
  }

  @Override
  protected byte[] endRow(Text rowKey) throws IOException {
    Record record = buildRecord(rowKey);

    if (filterRecord(rowKey, record))
      return null;

    // evaluate the filter against the record
    // Debugging
    // System.out.println(record);

    // serialize
    writer.write(record, encoder);
    encoder.flush();
    binaryBuffer.flush();

    return binaryBuffer.toByteArray();
  }

  private Record buildRecord(Text rowKey) {
    // populate root record
    for (SubGenericRecordBuilder nestedRecordBuilder : columnFamilyRecordBuilder.values())
      nestedRecordBuilder.endRow();

    // execute expressions
    if (!this.expressionColumns.isEmpty()) {
      // need to build it now, so that we can access the fields from within the
      // expressions
      Record record = rootRecordBuilder.build();
      expressionContext.setCurrent(rowKey, record);

      for (ExpressionColumn expr : this.expressionColumns)
        expr.populateField(rootRecordBuilder);
    }

    return rootRecordBuilder.build();
  }

  private boolean filterRecord(Text rowKey, Record record) {
    if (filterExpression != null) {
      expressionContext.setCurrent(rowKey, record);

      if (!(boolean) filterExpression.getValue(expressionContext))
        return true;
    }

    return false;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {

    super.init(source, options, env);

    this.initExpressionColumns(options);

    this.initAvro();

    this.initExpressions(options);

    this.initMLeapBundle(options);
  }

  private void initExpressionColumns(Map<String, String> options) {
    // expression setup
    // options: column.<name>.<type>, JUEL expression

    this.expressionColumns = new ArrayList<>();

    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (!entry.getKey().startsWith(COLUMN_PREFIX))
        continue;

      String[] arr = entry.getKey().split("\\.");
      if (arr.length != 3)
        throw new IllegalArgumentException(
            "Unable to parse column specification. column.<name>.<type>: " + entry.getKey());

      ExpressionColumn expr = new ExpressionColumn(arr[1], arr[2], entry.getValue());
      this.expressionColumns.add(expr);
    }
  }

  private void initAvro() {
    // merge schema based list and expression list
    Map<String, ExpressionColumn> expressionColumnMapping = new HashMap<>();
    List<SchemaMappingField> allSchemaMappingFields = new ArrayList<>(Arrays.asList(schemaMappingFields));

    for (ExpressionColumn expr : this.expressionColumns) {
      expressionColumnMapping.put(expr.getColumn(), expr);
      allSchemaMappingFields.add(expr.getSchemaMappingField());
    }

    // avro serialization setup
    schema = AvroUtil.buildSchema(allSchemaMappingFields);

    writer = new SpecificDatumWriter<>(schema);
    encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);

    // separate record builder for the root record holding the nested schemas
    rootRecordBuilder = new GenericRecordBuilder(schema);

    // setup GenericRecordBuilder for each column family
    for (Field field : schema.getFields()) {
      Schema nestedSchema = field.schema();

      // check if the field belongs to an expression
      ExpressionColumn expr = expressionColumnMapping.get(field.name());
      if (expr != null) {
        expr.setField(field);
        continue;
      }

      // field is a data field that needs collection from cells
      // nested vs top level element
      SubGenericRecordBuilder builder = nestedSchema.getType() == Type.RECORD
          ? new NestedGenericRecordBuilder(rootRecordBuilder, nestedSchema, field)
          : new TopLevelGenericRecordBuilder(rootRecordBuilder, field);

      // store in map for fast lookup
      columnFamilyRecordBuilder.put(new Text(field.name()), builder);
    }
  }

  private void initExpressions(Map<String, String> options) {
    this.expressionContext = new AvroContext(schema, schemaMappingFields);

    ExpressionFactory factory = ExpressionFactory.newInstance();

    for (ExpressionColumn expr : this.expressionColumns)
      expr.initialize(factory, expressionContext);

    // filter setup
    String filter = options.get(FILTER);
    if (filter != null)
      this.filterExpression = factory.createValueExpression(expressionContext, filter, boolean.class);
  }

  private void initMLeapBundle(Map<String, String> options) {
  }
}
