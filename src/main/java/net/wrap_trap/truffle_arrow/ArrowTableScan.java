package net.wrap_trap.truffle_arrow;

import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;

/**
 * TableScan for Apache Arrow
 */
public class ArrowTableScan extends TableScan implements ArrowRel {

  private RelOptTable relOptTable;
  private ArrowTable arrowTable;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private List<? extends RexNode> projects;
  private int[] fields;

  public ArrowTable getArrowTable() {
    return this.arrowTable;
  }

  public ArrowTableScan(RelOptCluster cluster, RelOptTable relOptTable, ArrowTable arrowTable,
                        VectorSchemaRoot[] vectorSchemaRoots, List<? extends RexNode> projects,
                        int[] fields) {
    super(cluster, cluster.traitSetOf(ArrowRel.CONVENTION), relOptTable);
    this.relOptTable = relOptTable;
    this.arrowTable = arrowTable;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.projects = projects;
    this.fields = fields;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ArrowTableScan(getCluster(), this.relOptTable, this.arrowTable, this.vectorSchemaRoots,
      this.projects, this.fields);
  }

  @Override
  public RelWriter explainTerms(RelWriter rw) {
    return super.explainTerms(rw).item("fields", this.projects);
  }

  @Override
  public RelDataType deriveRowType() {
    List<RelDataTypeField> fieldList = this.relOptTable.getRowType().getFieldList();
    RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
    Arrays.stream(this.fields).forEach(i -> builder.add(fieldList.get(i)));
//    Arrays.stream(this.getProjectFields(this.projects))
//      .forEach(i -> builder.add(fieldList.get(i)));
    return builder.build();
  }

  public VectorSchemaRoot[] getVectorSchemaRoots() {
    return this.vectorSchemaRoots;
  }

  public RelNode getInput() {
    return null;
  }

  public ThenRowSink createRowSink(ThenRowSink next, SinkContext context) {
    return
      frameDescriptor -> VectorSchemaRootBroker.compile(
        frameDescriptor,
        getRowType(),this.vectorSchemaRoots,
        this.projects,
        this.fields,
        context,
        next);
  }
}
