package net.wrap_trap.truffle_arrow.storage.columnar;

import net.wrap_trap.truffle_arrow.ArrowPreparingTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class ArrowColumnarTableScanRule extends RelOptRule {

  public static ArrowColumnarTableScanRule INSTANCE = new ArrowColumnarTableScanRule(RelFactories.LOGICAL_BUILDER);

  public ArrowColumnarTableScanRule(RelBuilderFactory relBuilderFactory) {
    super(
      operand(LogicalTableScan.class, none()),
      relBuilderFactory,
      "ArrowColumnarTableScanRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan scan = call.rel(0);
    RelOptTable relOptTable = scan.getTable();
    assert relOptTable instanceof ArrowPreparingTable;
    ArrowPreparingTable table = (ArrowPreparingTable) relOptTable;
    int[] fields = getProjectFields(scan.getChildExps());

    call.transformTo(
      new ArrowColumnarTableScan(
        scan.getCluster(),
        relOptTable,
        table.getTableDirectory(),
        table.getSchema(),
        scan.getChildExps(),
        fields));
  }

  private int[] getProjectFields(List<? extends RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }
}
