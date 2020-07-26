package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;
import java.util.stream.Collectors;

public class AggregateSink extends RowSink {

  public static AggregateSink createSink(
    FrameDescriptorPart framePart,
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RelNode input,
    RexBuilder rexBuilder,
    SinkContext context,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    for (int i = 0; i < groupSet.toList().size(); i ++) {
      newFramePart.addFrameSlot();
    }
    RowSink rowSink = next.apply(newFramePart);


    AggregateCall call = aggCalls.get(0);
    newFramePart.addFrameSlot();
    FrameSlot slot = newFramePart.findFrameSlot(newFramePart.getCurrentSlotPosition());
    newFramePart.frame().setFrameSlotKind(slot, FrameSlotKind.Long);

    RexNode rexNode = rexBuilder.makeCall(call.getAggregation(), Lists.newArrayList());
    RexCall rexCall = (RexCall) rexNode;
    RexCall newRexCall = rexCall.clone(rexCall.getType(), Lists.newArrayList(new RexFrameSlotRef(1)));

    ExprBase expr = compile(newFramePart, newRexCall, context);

    return new AggregateSink(
      newFramePart, indicator, groupSet, groupSets, aggCalls, Lists.newArrayList(expr), context, rowSink);
  }

  private FrameDescriptorPart framePart;
  private boolean indicator;
  private ImmutableBitSet groupSet;
  private ImmutableList<ImmutableBitSet> groupSets;
  private List<AggregateCall> aggCalls;
  private List<ExprBase> exprs;
  private SinkContext sinkContext;
  private RowSink then;
  private Map<List<Object>, Object> map;
  private TANewObject object;
  private FrameSlot tempFrameSlot;
  private ExprBase count;

  private AggregateSink(
    FrameDescriptorPart framePart,
    boolean indicator,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    List<ExprBase> exprs,
    SinkContext sinkContext,
    RowSink then) {
    this.framePart = framePart;
    this.indicator = indicator;
    this.groupSet = groupSet;
    this.groupSets = groupSets;
    this.aggCalls = aggCalls;
    this.exprs = exprs;
    this.sinkContext = sinkContext;
    this.then = then;
    this.map = new HashMap<>();
    this.object = TANewObjectNodeGen.create();
    this.tempFrameSlot = this.framePart.frame().addFrameSlot("tmpKey", FrameSlotKind.Object);

    // 複数の statement を作って実行するカンジ？
    // if (aa == null) {
    //   writeProperty(1, object)
    // } else {
    //   a = readProperty(object, 'a')
    //   writeProperty(a + 1, object)
    // }

    ExprBase key = ExprReadLocalNodeGen.create(this.tempFrameSlot);
    ExprBase hasMember = ExprHasMemberNodeGen.create(this.object, key);

    ExprBase readProperty = ExprReadPropertyNodeGen.create(this.object, key);
    ExprBase inc = ExprPlusNodeGen.create(readProperty, ExprLiteral.Int(1));
    ExprBase writeProperty = ExprWritePropertyNodeGen.create(this.object, key, inc);

    ExprBase initProperty = ExprWritePropertyNodeGen.create(this.object, key, ExprLiteral.Int(0));

    this.count = new ExprIf(hasMember, writeProperty, initProperty);

  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, SinkContext context) {
    return ProjectCompileExpr.compile(framePart, child, context);
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    List<Object> grouping = new ArrayList<>();
    for (Integer i : groupSet) {
      FrameSlot slot = this.framePart.findFrameSlotInPrevious(i);
      Objects.requireNonNull(slot);
      Object value = frame.getValue(slot);
      grouping.add(value);
    }
    StatementWriteLocalNodeGen.create(
      ExprLiteral.Object(grouping.toString()), this.tempFrameSlot).executeVoid(frame);


//    List<StatementWriteLocal> locals = map.get(grouping);
//    if (locals == null) {
//      int index = 0;
//      for (Object part: grouping) {
//        FrameSlot newSlot = this.framePart.findFrameSlot(index ++);
//        locals.add(StatementWriteLocalNodeGen.create(ExprLiteral.Object(part), newSlot));
//      }
//    }
//    int index = this.groupSet.toList().size();
//    for (ExprBase expr: this.exprs) {
//      FrameSlot newSlot = this.framePart.findFrameSlot(index ++);
//      locals.add(StatementWriteLocalNodeGen.create(expr, newSlot));
//    }
    map.put(grouping, this.count.executeGeneric(frame));
  }

  private static RelDataType getFieldType(RelNode relNode, int i) {
    final RelDataTypeField inputField =
      relNode.getRowType().getFieldList().get(i);
    return inputField.getType();
  }

//  @Override
//  public void executeVoid(VirtualFrame frame, SinkContext context)
//    throws UnexpectedResultException {
//    final List<RelDataType> argTypes =
//      call.getOperator() instanceof SqlCountAggFunction
//        ? new ArrayList<>(call.getOperandList().size())
//        : null;
//    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();
//    RexNode rexNode = rexBuilder.addAggCall(this.aggCalls.get(0), this.groupSet.size(), this.aggCalls, aggCallMapping, argTypes);
//
//
//  }

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    for (List<Object> keyList : this.map.keySet()) {
      int i;
      for (i = 0; i < keyList.size(); i ++) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(keyList.get(0)), this.framePart.findFrameSlot(i)).executeVoid(frame);
      }
      StatementWriteLocalNodeGen.create(
        ExprLiteral.Object(this.map.get(keyList)), this.framePart.findFrameSlot(i ++)).executeVoid(frame);

      then.executeByRow(frame, this.framePart, context);
    }
    then.afterExecute(frame, context);
  }
}
