package net.wrap_trap.truffle_arrow;

import com.google.common.base.Preconditions;
import net.wrap_trap.truffle_arrow.truffle.*;
import net.wrap_trap.truffle_arrow.truffle.node.GatherMergeNode;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class GatherMerge extends Aggregate implements ArrowRel {

  public GatherMerge(RelOptCluster cluster,
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
  public GatherMerge copy(
    RelTraitSet traitSet,
    RelNode input,
    ImmutableBitSet groupSet,
    List<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls) {
    try {
      return new GatherMerge(getCluster(), traitSet, input, this.indicator, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public ThenRowSink createRowSink(ThenRowSink next, CompileContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ThenLeader createLeader(ThenRowSink next, CompileContext context) {
    return () -> {
      FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
      SinkContext sinkContext = new SinkContext(context.getInputRefSlotMaps(), null, new ArrayList<Row>());
      return GatherMergeNode.createSink(framePart, this.groupSet, this.groupSets, this.aggCalls, this.getRowType(), context, next);
    };
  }

  @Override
  public boolean isLeader() {
    return true;
  }
}
