package net.wrap_trap.truffle_arrow;


import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ArrowProject  extends Project implements ArrowRel {

  private List<RelDataTypeField> inputFieldList;

  public ArrowProject(RelOptCluster cluster,
                      RelTraitSet traitSet,
                      RelNode input,
                      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    this.inputFieldList = input.getRowType().getFieldList();
  }

  @Override
  public ArrowProject copy(RelTraitSet traitSet, RelNode input,
                           List<RexNode> projects, RelDataType rowType) {
    return new ArrowProject(getCluster(), traitSet, input, projects, rowType);
  }

  public RelNode getInput() {
    return this.input;
  }

  public ThenRowSink createRowSink(ThenRowSink next) {
    int[] projectIndex = createProjectIndex();
    return
      sourceFrame -> ProjectSink.createSink(sourceFrame, projectIndex, next);
  }

  private int[] createProjectIndex() {
    List<String> fieldNames = getRowType().getFieldNames();
    int[] projectIndex = new int[fieldNames.size()];
    for (int i = 0; i < fieldNames.size(); i ++) {
      for (int j = 0; j < this.inputFieldList.size(); j ++) {
        RelDataTypeField inputField = this.inputFieldList.get(j);
        if (fieldNames.get(i).equals(inputField.getName())) {
          projectIndex[i] = inputField.getIndex();
          break;
        }
      }
    }
    return projectIndex;
  }
}
