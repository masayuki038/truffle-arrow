package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class ArrowPreparingTable extends Prepare.AbstractPreparingTable {

  private List<String> names;
  private ArrowTable table;
  private RelDataTypeFactory typeFactory;

  public ArrowPreparingTable(List<String> names, ArrowTable table,
                             RelDataTypeFactory typeFactory) {
    this.names = names;
    this.table = table;
    this.typeFactory = typeFactory;
  }

  @Override
  protected RelOptTable extend(Table extendedTable) {
    return null;
  }

  @Override
  public List<String> getQualifiedName() {
    return this.names;
  }

  @Override
  public double getRowCount() {
    return 0;
  }

  @Override
  public RelDataType getRowType() {
    return this.table.getRowType(this.typeFactory);
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return null;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return this.table.toRel(context, this);
  }

  @Override
  public List<RelCollation> getCollationList() {
    return ImmutableList.of(RelCollations.EMPTY);
  }

  @Override
  public RelDistribution getDistribution() {
    return null;
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    return false;
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return null;
  }

  @Override
  public Expression getExpression(Class clazz) {
    return null;
  }

  @Override
  public SqlMonotonicity getMonotonicity(String columnName) {
    return null;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return null;
  }

  @Override
  public boolean supportsModality(SqlModality modality) {
    return false;
  }

  @Override
  public boolean isTemporal() {
    return false;
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    return null;
  }
}
