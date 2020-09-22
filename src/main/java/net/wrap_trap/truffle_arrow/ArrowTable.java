package net.wrap_trap.truffle_arrow;

import com.google.common.collect.Lists;
import org.apache.arrow.vector.VectorSchemaRoot;
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

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table for Apache Arrow
 */
public class ArrowTable extends AbstractArrowTable implements QueryableTable, TranslatableTable {

  private VectorSchemaRoot[] vectorSchemaRoots;
  private RelProtoDataType tProtoRowType;

  public ArrowTable(VectorSchemaRoot[] vectorSchemaRoots, RelProtoDataType tProtoRowType) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.tProtoRowType = tProtoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory);
    }
    return deduceRowType(this.vectorSchemaRoots[0], (JavaTypeFactory) typeFactory);
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
    int[] fields = identityList(fieldCount);
    return new ArrowTableScan(context.getCluster(), relOptTable, this,
      this.vectorSchemaRoots, Lists.newArrayList(), fields);
  }

  private RelDataType deduceRowType(VectorSchemaRoot vectorSchemaRoot, JavaTypeFactory typeFactory) {
    List<Pair<String, RelDataType>> ret = vectorSchemaRoot.getFieldVectors().stream().map(fieldVector -> {
      RelDataType relDataType = ArrowFieldType.of(fieldVector.getField().getType()).toType(typeFactory);
      return new Pair<>(fieldVector.getField().getName(), relDataType);
    }).collect(Collectors.toList());
    return typeFactory.createStructType(ret);
  }

  static int[] identityList(int n) {
    int[] ret = new int[n];
    for (int i = 0; i < n; i++) {
      ret[i] = i;
    }
    return ret;
  }
}
