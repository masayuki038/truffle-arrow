package net.wrap_trap.truffle_arrow;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

public class ArrowProjectRule extends RelOptRule {
  public static ArrowProjectRule INSTANCE = new ArrowProjectRule(RelFactories.LOGICAL_BUILDER);

  public ArrowProjectRule(RelBuilderFactory relBuilderFactory) {
    super(
      operand(LogicalProject.class, any()),
      relBuilderFactory,
      "ArrowProjectRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);

    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    final RelDataType inputRowType = project.getRowType();
    final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);

    programBuilder.addIdentity();
    final RelTraitSet traitSet = project.getTraitSet().replace(ArrowRel.CONVENTION);
    ArrowProject newArrowProject = new ArrowProject(
      project.getCluster(),
      traitSet,
      convert(project.getInput(), ArrowRel.CONVENTION),
      project.getProjects(),
      project.getRowType());
    call.transformTo(newArrowProject);
  }
}
