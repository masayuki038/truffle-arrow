package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class AggregateSink extends RowSink {

  public static AggregateSink createSink(
    FrameDescriptor frameDescriptor,
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RexBuilder rexBuilder,
    SinkContext context,
    ThenRowSink next) {
    RowSink rowSink = next.apply(frameDescriptor);
    return new AggregateSink(indicator, groupSet, groupSets, aggCalls, rexBuilder, context, rowSink);
  }

  private boolean indicator;
  private ImmutableBitSet groupSet;
  private ImmutableList<ImmutableBitSet> groupSets;
  private List<AggregateCall> aggCalls;
  private RexBuilder rexBuilder;
  private SinkContext sinkContext;
  private RowSink then;

  private AggregateSink(
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RexBuilder rexBuilder,
    SinkContext sinkContext,
    RowSink then) {
    this.indicator = indicator;
    this.groupSet = groupSet;
    this.groupSets = groupSets;
    this.aggCalls = aggCalls;
    this.rexBuilder = rexBuilder;
    this.sinkContext = sinkContext;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor, SinkContext context)
    throws UnexpectedResultException {
    System.out.println("hoge");
//    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();
//    rexBuilder.addAggCall(this.aggCalls, this.groupSet.size(), this.aggCalls, aggCallMapping, )
  }
}
