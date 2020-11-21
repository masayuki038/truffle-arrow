package net.wrap_trap.truffle_arrow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class ArrowAggregateRule extends ConverterRule {

  public static ArrowAggregateRule INSTANCE = new ArrowAggregateRule();

  ArrowAggregateRule() {
    super(LogicalAggregate.class, Convention.NONE,
      ArrowRel.CONVENTION, "ArrowAggreagetRule");
  }

  public RelNode convert(RelNode rel) {
    final LogicalAggregate agg = (LogicalAggregate) rel;
    final RelTraitSet traitSet = agg.getTraitSet().replace(ArrowRel.CONVENTION);
    try {
      ArrowRel aggregate = new ArrowAggregate(
        rel.getCluster(),
        traitSet,
        convert(agg.getInput(), ArrowRel.CONVENTION),
        agg.indicator,
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList());

      return new GatherMerge(
        rel.getCluster(),
        traitSet,
        aggregate
      );
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }
}
