package net.wrap_trap.truffle_arrow;

import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import net.wrap_trap.truffle_arrow.truffle.VectorSchemaRootBroker;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Arrays;
import java.util.List;

/**
 * TableScan for Apache Arrow
 */
public class ArrowTableScan extends TableScan implements ArrowRel {

  private RelOptTable relOptTable;
  private ArrowTable arrowTable;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] fields;

  public ArrowTable getArrowTable() {
    return this.arrowTable;
  }

  public ArrowTableScan(RelOptCluster cluster, RelOptTable relOptTable, ArrowTable arrowTable, VectorSchemaRoot[] vectorSchemaRoots, int[] fields) {
    super(cluster, cluster.traitSetOf(ArrowRel.CONVENTION), relOptTable);
    this.relOptTable = relOptTable;
    this.arrowTable = arrowTable;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ArrowTableScan(getCluster(), this.relOptTable, this.arrowTable, this.vectorSchemaRoots, this.fields);
  }

  @Override
  public RelWriter explainTerms(RelWriter rw) {
    return super.explainTerms(rw).item("fields", Primitive.asList(this.fields));
  }

  @Override
  public RelDataType deriveRowType() {
    List<RelDataTypeField> fieldList = this.relOptTable.getRowType().getFieldList();
    RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
    Arrays.stream(this.fields).forEach(i -> builder.add(fieldList.get(i)));
    return builder.build();
  }

  public VectorSchemaRoot[] getVectorSchemaRoots() {
    return this.vectorSchemaRoots;
  }

  public RowSource compile(ThenRowSink next) {
    return VectorSchemaRootBroker.compile(getRowType(), this.vectorSchemaRoots, this.fields, next);
  }
}
