package net.wrap_trap.truffle_arrow;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Created by masayuki on 2019/12/06.
 */
public class ArrowPreparingTable extends Prepare.AbstractPreparingTable {

  private Table table;

  public ArrowPreparingTable(Table table) {
    this.table = table;
  }

  @Override
  protected RelOptTable extend(Table extendedTable) {
    return null;
  }

  @Override
  public List<String> getQualifiedName() {
    return null;
  }

  @Override
  public double getRowCount() {
    return 0;
  }

  @Override
  public RelDataType getRowType() {
    return null;
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return null;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return null;
  }

  @Override
  public List<RelCollation> getCollationList() {
    return null;
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
