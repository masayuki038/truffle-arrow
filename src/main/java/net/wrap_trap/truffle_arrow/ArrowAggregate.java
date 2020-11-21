package net.wrap_trap.truffle_arrow;

import com.google.common.base.Preconditions;
import net.wrap_trap.truffle_arrow.truffle.AggregateSink;
import net.wrap_trap.truffle_arrow.truffle.CompileContext;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class ArrowAggregate extends Aggregate implements ArrowRel {

  public ArrowAggregate(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelNode child,
    boolean indicator,
    ImmutableBitSet groupSet,
    List<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    Preconditions.checkArgument(!indicator,
      "ArrowAggregate no longer supports indicator fields");

    for (AggregateCall aggCall: aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException("distinct aggregation not supported");
      }
      AggImplementor implementor2 = RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
      if (implementor2 == null) {
        throw new InvalidRelException("aggregation " + aggCall.getAggregation() + " not supported");
      }
    }
  }

  @Override
  public ArrowAggregate copy(
    RelTraitSet traitSet,
    RelNode input,
    ImmutableBitSet groupSet,
    List<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls) {
    try {
      return new ArrowAggregate(getCluster(), traitSet, input, this.indicator, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public ThenRowSink createRowSink(ThenRowSink next, CompileContext context) {
    return
      sourceFrame -> AggregateSink.createSink(
        sourceFrame, this.groupSet, this.groupSets, this.aggCalls, context, next);
  }
}
