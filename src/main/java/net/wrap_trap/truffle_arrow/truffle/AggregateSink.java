package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;

public class AggregateSink extends RowSink {

  public static AggregateSink createSink(
    FrameDescriptorPart framePart,
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RexBuilder rexBuilder,
    SinkContext context,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    for (int i = 0; i < groupSet.size(); i ++) {
      newFramePart.addFrameSlot();
    }
    RowSink rowSink = next.apply(newFramePart);
    return new AggregateSink(newFramePart, indicator, groupSet, groupSets, aggCalls, rexBuilder, context, rowSink);
  }

  private FrameDescriptorPart framePart;
  private boolean indicator;
  private ImmutableBitSet groupSet;
  private ImmutableList<ImmutableBitSet> groupSets;
  private List<AggregateCall> aggCalls;
  private RexBuilder rexBuilder;
  private SinkContext sinkContext;
  private RowSink then;
  private Map<List<Object>, List<StatementWriteLocal>> map;

  private AggregateSink(
    FrameDescriptorPart framePart,
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RexBuilder rexBuilder,
    SinkContext sinkContext,
    RowSink then) {
    this.framePart = framePart;
    this.indicator = indicator;
    this.groupSet = groupSet;
    this.groupSets = groupSets;
    this.aggCalls = aggCalls;
    this.rexBuilder = rexBuilder;
    this.sinkContext = sinkContext;
    this.then = then;
    this.map = new HashMap<>();
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    // filter でいうところの、"$1=1"のような条件式は無い
    // その代わりとなる何かで ExprBase を作る
    // その一つの方法が、rexBuilder.addAggCall
    //   一つ前のslot から値を取り出す
    //  allCalls に値があれば、そこから値を取り出して関数を実行する
    // map に詰める
    List<Object> grouping = new ArrayList<>();
    List<StatementWriteLocal> locals = new ArrayList<>();
    int index = 0;
    for (Integer i : groupSet) {
      FrameSlot slot = this.framePart.findFrameSlotInPrevious(i);
      Objects.requireNonNull(slot);
      Object value = frame.getValue(slot);
      grouping.add(value);
      FrameSlot newSlot = this.framePart.findFrameSlot(index ++);
      locals.add(StatementWriteLocalNodeGen.create(ExprLiteral.Object(value), newSlot));
    }
    map.put(grouping, locals);
  }

//  @Override
//  public void executeVoid(VirtualFrame frame, SinkContext context)
//    throws UnexpectedResultException {
//    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();
//    rexBuilder.addAggCall(this.aggCalls, this.groupSet.size(), this.aggCalls, aggCallMapping, )
//    final int nGroups = oldAggRel.getGroupCount();
//    List<RelDataType> oldArgTypes =
//      SqlTypeUtil.projectTypes(
//        oldAggRel.getInput().getRowType(), oldCall.getArgList());
//    return rexBuilder.addAggCall(oldCall,
//      nGroups,
//      aggCalls,
//      aggCallMapping,
//      oldArgTypes);
//
//  }

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    for(List<Object> key : this.map.keySet()) {
      List<StatementWriteLocal> locals = this.map.get(key);
      for (StatementWriteLocal local : locals) {
        local.executeVoid(frame);
      }
      then.executeByRow(frame, this.framePart, context);
    }
    then.afterExecute(frame, context);
  }
}
