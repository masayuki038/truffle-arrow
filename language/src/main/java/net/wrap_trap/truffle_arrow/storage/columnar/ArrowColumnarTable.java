package net.wrap_trap.truffle_arrow.storage.columnar;

import com.google.common.collect.Lists;
import net.wrap_trap.truffle_arrow.AbstractArrowTable;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Pair;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class ArrowColumnarTable extends AbstractArrowTable implements QueryableTable, TranslatableTable {

  private File dir;
  private Schema schema;
  private RelProtoDataType tProtoRowType;

  public ArrowColumnarTable(File dir, RelProtoDataType tProtoRowType) {
    this.dir = dir;

    File schemaFile = new File(dir, "schema.json");
    if (!schemaFile.exists() || !schemaFile.isFile()) {
      throw new IllegalStateException("Missing schema.json under your table folder");
    }

    try {
      List<String> lines = Files.readAllLines(schemaFile.toPath());
      this.schema = Schema.fromJSON(String.join("", lines));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.tProtoRowType = tProtoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory);
    }
    return deduceRowType((JavaTypeFactory) typeFactory);
  }

  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    int fieldCount = relOptTable.getRowType().getFieldCount();
    return new ArrowColumnarTableScan(context.getCluster(), relOptTable, this.dir,
      this.schema, Lists.newArrayList());
  }

  @Override
  public File getTableDirectory() {
    return this.dir;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  private RelDataType deduceRowType(JavaTypeFactory typeFactory) {
    List<Pair<String, RelDataType>> ret = this.schema.getFields().stream().map(field -> {
      RelDataType relDataType = ArrowFieldType.of(field.getType()).toType(typeFactory);
      return new Pair<>(field.getName(), relDataType);
    }).collect(Collectors.toList());
    return typeFactory.createStructType(ret);
  }
}
