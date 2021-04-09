package net.wrap_trap.truffle_arrow;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

/**
 * Filter scan rule for Apache Arrow
 */
public class ArrowFilterTableScanRule extends RelOptRule {

  public static ArrowFilterTableScanRule INSTANCE = new ArrowFilterTableScanRule();

  public ArrowFilterTableScanRule() {
    super(operand(LogicalFilter.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final RelNode input = filter.getInput();

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelDataType inputRowType = input.getRowType();
    final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
    programBuilder.addIdentity();
    programBuilder.addCondition(filter.getCondition());
    final RexProgram program = programBuilder.getProgram();

    final RelTraitSet traitSet = filter.getTraitSet().replace(ArrowRel.CONVENTION);
    RexNode rexNode = filter.getCondition();
    final ArrowFilter arrowFilter = ArrowFilter.create(
      traitSet,
      convert(input, ArrowRel.CONVENTION),
      program,
      rexNode);
    call.transformTo(arrowFilter);
  }
}
